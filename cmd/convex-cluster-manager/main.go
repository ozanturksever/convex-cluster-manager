// Package main provides the CLI entry point for convex-cluster-manager.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ozanturksever/convex-cluster-manager/internal/cluster"
	"github.com/ozanturksever/convex-cluster-manager/internal/config"
	"github.com/ozanturksever/convex-cluster-manager/internal/health"
	"github.com/ozanturksever/convex-cluster-manager/internal/replication"
)

const (
	defaultConfigPath = "/etc/convex/cluster.json"
)

var (
	// Version information set via ldflags
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

var (
	configPath string
	cfg        *config.Config
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "convex-cluster-manager",
	Short: "Convex Cluster Manager - Active-passive clustering for Convex backends",
	Long: `Convex Cluster Manager provides active-passive clustering for Convex backends
using NATS for coordination and Litestream for SQLite WAL replication.

It manages leader election, VIP failover, and backend service control.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip config loading for init command
		if cmd.Name() == "init" || cmd.Name() == "version" || cmd.Name() == "help" {
			return nil
		}

		// Load configuration
		var err error
		cfg, err = config.LoadFromFile(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		cfg.ApplyDefaults()

		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", defaultConfigPath, "Path to cluster configuration file")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(joinCmd)
	rootCmd.AddCommand(leaveCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(promoteCmd)
	rootCmd.AddCommand(demoteCmd)
	rootCmd.AddCommand(daemonCmd)
	rootCmd.AddCommand(versionCmd)
}

// initCmd initializes cluster configuration
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize cluster configuration for this node",
	Long: `Initialize cluster configuration by creating a configuration file.

This command creates the cluster configuration file with the specified parameters.
After initialization, you can start the daemon or join an existing cluster.`,
	RunE: runInit,
}

var (
	initClusterID  string
	initNodeID     string
	initNATSURLs   []string
	initVIPAddress string
	initVIPNetmask int
	initVIPIface   string
)

func init() {
	initCmd.Flags().StringVar(&initClusterID, "cluster-id", "", "Cluster identifier (required)")
	initCmd.Flags().StringVar(&initNodeID, "node-id", "", "Node identifier (required)")
	initCmd.Flags().StringSliceVar(&initNATSURLs, "nats", nil, "NATS server URLs (required)")
	initCmd.Flags().StringVar(&initVIPAddress, "vip", "", "Virtual IP address (e.g., 192.168.1.100)")
	initCmd.Flags().IntVar(&initVIPNetmask, "vip-netmask", 24, "VIP netmask (CIDR notation)")
	initCmd.Flags().StringVar(&initVIPIface, "interface", "", "Network interface for VIP")

	_ = initCmd.MarkFlagRequired("cluster-id")
	_ = initCmd.MarkFlagRequired("node-id")
	_ = initCmd.MarkFlagRequired("nats")
}

func runInit(cmd *cobra.Command, args []string) error {
	// Create configuration
	newCfg := &config.Config{
		ClusterID: initClusterID,
		NodeID:    initNodeID,
		NATS: config.NATSConfig{
			Servers: initNATSURLs,
		},
	}

	// Add VIP config if provided
	if initVIPAddress != "" {
		if initVIPIface == "" {
			return fmt.Errorf("--interface is required when --vip is specified")
		}
		newCfg.VIP = config.VIPConfig{
			Address:   initVIPAddress,
			Netmask:   initVIPNetmask,
			Interface: initVIPIface,
		}
	}

	// Apply defaults
	newCfg.ApplyDefaults()

	// Validate
	if err := newCfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Create config directory based on the actual config path
	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Write configuration file
	data, err := marshalConfig(newCfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	fmt.Printf("✓ Cluster configuration initialized\n")
	fmt.Printf("  Config file: %s\n", configPath)
	fmt.Printf("  Cluster ID:  %s\n", initClusterID)
	fmt.Printf("  Node ID:     %s\n", initNodeID)
	fmt.Printf("  NATS:        %v\n", initNATSURLs)
	if initVIPAddress != "" {
		fmt.Printf("  VIP:         %s/%d on %s\n", initVIPAddress, initVIPNetmask, initVIPIface)
	}
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Start the daemon:  convex-cluster-manager daemon")
	fmt.Println("  2. Or join cluster:   convex-cluster-manager join")

	return nil
}

// marshalConfig converts Config to JSON with millisecond durations
func marshalConfig(c *config.Config) ([]byte, error) {
	raw := map[string]interface{}{
		"clusterId": c.ClusterID,
		"nodeId":    c.NodeID,
		"nats": map[string]interface{}{
			"servers": c.NATS.Servers,
		},
		"election": map[string]interface{}{
			"leaderTtlMs":         c.Election.LeaderTTL.Milliseconds(),
			"heartbeatIntervalMs": c.Election.HeartbeatInterval.Milliseconds(),
		},
		"wal": map[string]interface{}{
			"streamRetention":    c.WAL.StreamRetention.String(),
			"snapshotIntervalMs": c.WAL.SnapshotInterval.Milliseconds(),
			"replicaPath":        c.WAL.ReplicaPath,
		},
		"backend": map[string]interface{}{
			"serviceName":    c.Backend.ServiceName,
			"healthEndpoint": c.Backend.HealthEndpoint,
			"dataPath":       c.Backend.DataPath,
		},
	}

	if c.VIP.Address != "" {
		raw["vip"] = map[string]interface{}{
			"address":   c.VIP.Address,
			"netmask":   c.VIP.Netmask,
			"interface": c.VIP.Interface,
		}
	}

	if c.NATS.Credentials != "" {
		raw["nats"].(map[string]interface{})["credentials"] = c.NATS.Credentials
	}

	return json.MarshalIndent(raw, "", "  ")
}

// joinCmd joins an existing cluster
var joinCmd = &cobra.Command{
	Use:   "join",
	Short: "Join an existing cluster and bootstrap from snapshot",
	Long: `Join an existing cluster by bootstrapping data from a snapshot.

This command connects to the cluster, downloads the latest snapshot,
and prepares the node to become a passive member.`,
	RunE: runJoin,
}

func runJoin(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fmt.Printf("Joining cluster %s as node %s...\n", cfg.ClusterID, cfg.NodeID)

	// Create election to connect to cluster
	election, err := cluster.NewElection(cluster.ElectionConfig{
		ClusterID:         cfg.ClusterID,
		NodeID:            cfg.NodeID,
		NATSURLs:          cfg.NATS.Servers,
		NATSCredentials:   cfg.NATS.Credentials,
		LeaderTTL:         cfg.Election.LeaderTTL,
		HeartbeatInterval: cfg.Election.HeartbeatInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create election: %w", err)
	}

	// Start election (will attempt to connect to NATS)
	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer election.Stop()

	// Wait briefly to detect current leader
	time.Sleep(2 * time.Second)

	leader := election.CurrentLeader()
	if leader == "" {
		fmt.Println("⚠ No leader detected. This node may become leader.")
	} else {
		fmt.Printf("✓ Connected to cluster. Current leader: %s\n", leader)
	}

	// Bootstrap data from cluster snapshot
	fmt.Println()
	fmt.Println("Bootstrapping data from cluster snapshot...")

	bootstrapCfg := replication.BootstrapConfig{
		ClusterID:       cfg.ClusterID,
		NATSURLs:        cfg.NATS.Servers,
		NATSCredentials: cfg.NATS.Credentials,
		OutputPath:      cfg.Backend.DataPath,
	}

	if err := replication.Bootstrap(ctx, bootstrapCfg); err != nil {
		if errors.Is(err, replication.ErrNoSnapshots) {
			fmt.Println("⚠ No snapshots available yet. This node will start with empty data.")
			fmt.Println("  (This is normal for new clusters or if no primary has taken a snapshot yet.)")
		} else {
			return fmt.Errorf("failed to bootstrap data: %w", err)
		}
	} else {
		fmt.Printf("✓ Data bootstrapped to %s\n", cfg.Backend.DataPath)
	}

	fmt.Println()
	fmt.Println("✓ Successfully joined cluster")
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  Start the daemon: convex-cluster-manager daemon")

	return nil
}

// leaveCmd leaves the cluster gracefully
var leaveCmd = &cobra.Command{
	Use:   "leave",
	Short: "Leave the cluster gracefully",
	Long: `Leave the cluster gracefully by stepping down if leader and
disconnecting from the coordination service.`,
	RunE: runLeave,
}

func runLeave(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("Leaving cluster %s...\n", cfg.ClusterID)

	// Create election to connect to cluster
	election, err := cluster.NewElection(cluster.ElectionConfig{
		ClusterID:         cfg.ClusterID,
		NodeID:            cfg.NodeID,
		NATSURLs:          cfg.NATS.Servers,
		NATSCredentials:   cfg.NATS.Credentials,
		LeaderTTL:         cfg.Election.LeaderTTL,
		HeartbeatInterval: cfg.Election.HeartbeatInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create election: %w", err)
	}

	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}

	// Wait briefly to check if we're leader
	time.Sleep(time.Second)

	if election.IsLeader() {
		fmt.Println("Stepping down from leadership...")
		if err := election.StepDown(ctx); err != nil {
			return fmt.Errorf("failed to step down: %w", err)
		}
	}

	election.Stop()

	fmt.Println("✓ Successfully left cluster")

	return nil
}

// statusCmd shows cluster status
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	Long:  `Show the current status of the cluster including role, leader, and VIP status.`,
	RunE:  runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Print header
	fmt.Println("Cluster Status")
	fmt.Println("==============")
	fmt.Println()
	fmt.Printf("Cluster ID:  %s\n", cfg.ClusterID)
	fmt.Printf("Node ID:     %s\n", cfg.NodeID)
	fmt.Printf("NATS:        %v\n", cfg.NATS.Servers)
	if cfg.VIP.Address != "" {
		fmt.Printf("VIP:         %s/%d\n", cfg.VIP.Address, cfg.VIP.Netmask)
	}
	fmt.Println()

	// Try to query the running daemon via NATS health checker first.
	// This gives us the actual daemon state without interfering with elections.
	healthCfg := health.Config{
		ClusterID: cfg.ClusterID,
		NodeID:    cfg.NodeID + "-status-query", // Use different node ID to not interfere
		NATSURLs:  cfg.NATS.Servers,
	}
	checker, err := health.NewChecker(healthCfg)
	if err != nil {
		return fmt.Errorf("failed to create health checker: %w", err)
	}

	if err := checker.Start(ctx); err != nil {
		return fmt.Errorf("failed to start health checker: %w", err)
	}
	defer checker.Stop()

	// Query this node's daemon health status
	resp, err := checker.QueryNode(ctx, cfg.NodeID, 5*time.Second)
	if err != nil {
		// If we can't reach the daemon, fall back to just showing the leader from KV
		fmt.Printf("Role:        UNKNOWN (daemon not responding)\n")
		fmt.Printf("Leader:      (could not query)\n")
		fmt.Println()
		fmt.Println("Note: The daemon may not be running on this node.")
		fmt.Println("      Start it with: convex-cluster-manager daemon")
		return nil
	}

	// Display daemon's reported status
	fmt.Printf("Role:        %s\n", resp.Role)
	leader := resp.Leader
	if leader == "" {
		leader = "(none)"
	}
	fmt.Printf("Leader:      %s\n", leader)

	if resp.BackendStatus != "" {
		fmt.Printf("Backend:     %s\n", resp.BackendStatus)
	}

	if resp.ReplicationLag > 0 {
		fmt.Printf("Rep. Lag:    %dms\n", resp.ReplicationLag)
	}

	// Check if VIP is held
	if cfg.VIP.Address != "" {
		vipStatus := "not held"
		if resp.Role == "PRIMARY" {
			vipStatus = "held (this node is PRIMARY)"
		}
		fmt.Printf("VIP Status:  %s\n", vipStatus)
	}

	return nil
}

// promoteCmd forces this node to become leader
var promoteCmd = &cobra.Command{
	Use:   "promote",
	Short: "Force this node to become leader",
	Long: `Force this node to become the cluster leader.

This command attempts to acquire leadership immediately. Use with caution
as it may cause temporary service disruption during failover.`,
	RunE: runPromote,
}

func runPromote(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("Attempting to promote node %s to leader...\n", cfg.NodeID)

	// Create election
	election, err := cluster.NewElection(cluster.ElectionConfig{
		ClusterID:         cfg.ClusterID,
		NodeID:            cfg.NodeID,
		NATSURLs:          cfg.NATS.Servers,
		NATSCredentials:   cfg.NATS.Credentials,
		LeaderTTL:         cfg.Election.LeaderTTL,
		HeartbeatInterval: cfg.Election.HeartbeatInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create election: %w", err)
	}

	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer election.Stop()

	// Wait for leadership with timeout
	select {
	case <-election.LeaderCh():
		fmt.Println("✓ Successfully promoted to leader")
		return nil
	case <-time.After(15 * time.Second):
		if election.IsLeader() {
			fmt.Println("✓ Successfully promoted to leader")
			return nil
		}
		leader := election.CurrentLeader()
		if leader != "" && leader != cfg.NodeID {
			return fmt.Errorf("failed to promote: another node (%s) is currently leader", leader)
		}
		return fmt.Errorf("failed to promote: timeout waiting for leadership")
	case <-ctx.Done():
		return fmt.Errorf("promotion cancelled")
	}
}

// demoteCmd releases leadership
var demoteCmd = &cobra.Command{
	Use:   "demote",
	Short: "Release leadership and become passive",
	Long: `Release leadership voluntarily and become a passive node.

This command gracefully steps down from leadership, allowing another
node to take over.`,
	RunE: runDemote,
}

func runDemote(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("Demoting node %s from leader...\n", cfg.NodeID)

	// Create election
	election, err := cluster.NewElection(cluster.ElectionConfig{
		ClusterID:         cfg.ClusterID,
		NodeID:            cfg.NodeID,
		NATSURLs:          cfg.NATS.Servers,
		NATSCredentials:   cfg.NATS.Credentials,
		LeaderTTL:         cfg.Election.LeaderTTL,
		HeartbeatInterval: cfg.Election.HeartbeatInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create election: %w", err)
	}

	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer election.Stop()

	// Wait briefly to sync with cluster state
	time.Sleep(time.Second)

	// Check if this node is the current leader by looking at the KV store.
	// Note: We check CurrentLeader() instead of IsLeader() because IsLeader()
	// only returns true if THIS Election instance acquired leadership.
	// The running daemon has its own Election instance that holds leadership.
	currentLeader := election.CurrentLeader()
	if currentLeader != cfg.NodeID {
		if currentLeader == "" {
			fmt.Println("No leader currently exists in the cluster")
		} else {
			fmt.Printf("This node is not the current leader (leader is %s)\n", currentLeader)
		}
		return nil
	}

	// This node is the leader according to KV. Call StepDown which will:
	// 1. Set a cooldown marker to prevent this node from re-acquiring leadership
	// 2. Delete the leader key from KV
	// The running daemon will detect the key deletion and transition to passive.
	if err := election.StepDown(ctx); err != nil {
		return fmt.Errorf("failed to step down: %w", err)
	}

	fmt.Println("✓ Successfully demoted from leader")
	fmt.Println("  Another node should acquire leadership within", cfg.Election.LeaderTTL)

	return nil
}

// daemonCmd runs the cluster daemon
var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run the cluster daemon",
	Long: `Run the cluster daemon which manages leader election, VIP failover,
and backend service control.

The daemon runs continuously until stopped with SIGINT or SIGTERM.`,
	RunE: runDaemon,
}

func runDaemon(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("Starting convex-cluster-manager daemon...")
	fmt.Printf("  Cluster ID: %s\n", cfg.ClusterID)
	fmt.Printf("  Node ID:    %s\n", cfg.NodeID)
	fmt.Printf("  NATS:       %v\n", cfg.NATS.Servers)
	if cfg.VIP.Address != "" {
		fmt.Printf("  VIP:        %s/%d on %s\n", cfg.VIP.Address, cfg.VIP.Netmask, cfg.VIP.Interface)
	}
	fmt.Println()

	daemon, err := cluster.NewDaemon(cfg)
	if err != nil {
		return fmt.Errorf("failed to create daemon: %w", err)
	}

	fmt.Println("✓ Daemon started. Press Ctrl+C to stop.")

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
	}()

	if err := daemon.Run(ctx); err != nil {
		return fmt.Errorf("daemon error: %w", err)
	}

	fmt.Println("✓ Daemon stopped")
	return nil
}

// versionCmd shows version information
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("convex-cluster-manager %s\n", version)
		fmt.Printf("  commit: %s\n", commit)
		fmt.Printf("  built:  %s\n", date)
	},
}
