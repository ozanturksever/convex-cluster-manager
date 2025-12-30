package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/ozanturksever/convex-cluster-manager/internal/backend"
	"github.com/ozanturksever/convex-cluster-manager/internal/config"
	"github.com/ozanturksever/convex-cluster-manager/internal/health"
	"github.com/ozanturksever/convex-cluster-manager/internal/replication"
	"github.com/ozanturksever/convex-cluster-manager/internal/vip"
)

// Daemon composes leader election, state management, backend control,
// VIP management, WAL replication (primary/passive), and the NATS-based
// health checker into a single long-running process.
type Daemon struct {
	cfg    *config.Config
	logger *slog.Logger

	state    *State
	election *Election

	backend     *backend.Controller
	vip         *vip.Manager
	primary     *replication.Primary
	passive     *replication.Passive
	snapshotter *replication.Snapshotter
	health      *health.Checker

	exec *systemExecutor

	ctxMu sync.RWMutex
	ctx   context.Context
}

// systemExecutor is a simple os/exec-backed implementation that satisfies
// both backend.Executor and vip.Executor.
type systemExecutor struct{}

func (e *systemExecutor) Execute(ctx context.Context, cmd string, args ...string) (string, error) {
	c := exec.CommandContext(ctx, cmd, args...)
	out, err := c.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// NewDaemon constructs a Daemon from the validated cluster configuration.
func NewDaemon(cfg *config.Config) (*Daemon, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	d := &Daemon{
		cfg: cfg,
		logger: slog.Default().With(
			"component", "daemon",
			"cluster", cfg.ClusterID,
			"node", cfg.NodeID,
		),
		exec: &systemExecutor{},
	}

	// Cluster state.
	d.state = NewState(cfg.NodeID, cfg.ClusterID)

	// Leader election via NATS KV.
	elCfg := ElectionConfig{
		ClusterID:         cfg.ClusterID,
		NodeID:            cfg.NodeID,
		NATSURLs:          cfg.NATS.Servers,
		NATSCredentials:   cfg.NATS.Credentials,
		LeaderTTL:         cfg.Election.LeaderTTL,
		HeartbeatInterval: cfg.Election.HeartbeatInterval,
	}

	var err error
	if d.election, err = NewElection(elCfg); err != nil {
		return nil, fmt.Errorf("create election: %w", err)
	}

	// Backend controller (systemd).
	backendCfg := backend.Config{
		ServiceName:    cfg.Backend.ServiceName,
		HealthEndpoint: cfg.Backend.HealthEndpoint,
		DataPath:       cfg.Backend.DataPath,
	}
	if err := backendCfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid backend config: %w", err)
	}
	d.backend = backend.NewController(backendCfg, d.exec)

	// VIP manager (optional).
	if cfg.VIP.Address != "" {
		vipCfg := vip.Config{
			Address:   cfg.VIP.Address,
			Netmask:   cfg.VIP.Netmask,
			Interface: cfg.VIP.Interface,
		}
		if err := vipCfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid VIP config: %w", err)
		}
		d.vip = vip.NewManager(vipCfg, d.exec)
	}

	// Primary WAL replication (leader only).
	primaryCfg := replication.Config{
		ClusterID:        cfg.ClusterID,
		NodeID:          cfg.NodeID,
		NATSURLs:        cfg.NATS.Servers,
		NATSCredentials: cfg.NATS.Credentials,
		DBPath:          cfg.Backend.DataPath,
		SnapshotInterval: cfg.WAL.SnapshotInterval,
		ReplicaPath:     "",
	}
	if d.primary, err = replication.NewPrimary(primaryCfg); err != nil {
		return nil, fmt.Errorf("create primary replication: %w", err)
	}

	// Passive WAL replication (non-leader catch-up).
	passiveCfg := replication.Config{
		ClusterID:        cfg.ClusterID,
		NodeID:          cfg.NodeID,
		NATSURLs:        cfg.NATS.Servers,
		NATSCredentials: cfg.NATS.Credentials,
		DBPath:          cfg.WAL.ReplicaPath,
		SnapshotInterval: cfg.WAL.SnapshotInterval,
		ReplicaPath:     "",
	}
	if d.passive, err = replication.NewPassive(passiveCfg); err != nil {
		return nil, fmt.Errorf("create passive replication: %w", err)
	}

	// Snapshot manager for full snapshots & retention.
	snapCfg := replication.SnapshotConfig{
		Config: replication.Config{
			ClusterID:        cfg.ClusterID,
			NodeID:          cfg.NodeID,
			NATSURLs:        cfg.NATS.Servers,
			NATSCredentials: cfg.NATS.Credentials,
			DBPath:          cfg.Backend.DataPath,
			SnapshotInterval: cfg.WAL.SnapshotInterval,
			ReplicaPath:     "",
		},
		Retention: cfg.WAL.StreamRetention,
	}
	if d.snapshotter, err = replication.NewSnapshotter(snapCfg); err != nil {
		return nil, fmt.Errorf("create snapshotter: %w", err)
	}

	// Health checker over NATS request/reply.
	hCfg := health.Config{
		ClusterID: cfg.ClusterID,
		NodeID:    cfg.NodeID,
		NATSURLs:  cfg.NATS.Servers,
	}
	if d.health, err = health.NewChecker(hCfg); err != nil {
		return nil, fmt.Errorf("create health checker: %w", err)
	}
	d.health.SetRole("PASSIVE")
	d.health.SetBackendStatus("stopped")

	// Wire state callbacks to side-effectful operations.
	d.state.OnBecomeLeader(func() {
		d.handleBecomeLeader()
	})

	d.state.OnStepDown(func() {
		d.handleStepDown()
	})

	d.state.OnLeaderChange(func(leader string) {
		d.health.SetLeader(leader)
	})

	return d, nil
}

// setCtx stores the root context used by the daemon.
func (d *Daemon) setCtx(ctx context.Context) {
	d.ctxMu.Lock()
	defer d.ctxMu.Unlock()
	d.ctx = ctx
}

// getCtx retrieves the stored context, falling back to Background.
func (d *Daemon) getCtx() context.Context {
	d.ctxMu.RLock()
	defer d.ctxMu.RUnlock()
	if d.ctx != nil {
		return d.ctx
	}
	return context.Background()
}

// handleBecomeLeader is invoked when this node transitions to PRIMARY.
// It starts the backend, acquires the VIP, enables primary replication
// and snapshots, and updates health state.
func (d *Daemon) handleBecomeLeader() {
	ctx := d.getCtx()

	d.logger.Info("becoming PRIMARY")
	d.health.SetRole("PRIMARY")
	d.health.SetLeader(d.cfg.NodeID)

	// Stop passive replication if running.
	if d.passive != nil {
		if err := d.passive.Stop(ctx); err != nil {
			d.logger.Error("failed to stop passive replication", "error", err)
		}
	}

	// Acquire VIP.
	if d.vip != nil {
		if err := d.vip.Acquire(ctx); err != nil {
			d.logger.Error("failed to acquire VIP", "error", err)
		} else {
			d.state.SetVIPAcquired(true)
		}
	}

	// Start backend service.
	if err := d.backend.Start(ctx); err != nil {
		d.logger.Error("failed to start backend service", "error", err)
		d.health.SetBackendStatus("failed")
	} else {
		d.health.SetBackendStatus("running")
	}

	// Start primary WAL replication.
	if err := d.primary.Start(ctx); err != nil {
		d.logger.Error("failed to start primary replication", "error", err)
	}

	// Start periodic snapshots.
	if err := d.snapshotter.Start(ctx); err != nil {
		d.logger.Error("failed to start snapshotter", "error", err)
	}
}

// handleStepDown is invoked when this node transitions from PRIMARY to
// PASSIVE. It stops primary-specific components, releases the VIP, and
// starts passive replication and an initial catch-up.
func (d *Daemon) handleStepDown() {
	ctx := d.getCtx()

	d.logger.Info("stepping down to PASSIVE")
	d.health.SetRole("PASSIVE")

	// Stop snapshotter first so no new snapshots are taken.
	if err := d.snapshotter.Stop(ctx); err != nil {
		d.logger.Error("failed to stop snapshotter", "error", err)
	}

	// Stop primary replication.
	if err := d.primary.Stop(ctx); err != nil {
		d.logger.Error("failed to stop primary replication", "error", err)
	}

	// Stop backend service.
	if err := d.backend.Stop(ctx); err != nil {
		d.logger.Error("failed to stop backend service", "error", err)
	}
	d.health.SetBackendStatus("stopped")

	// Release VIP.
	if d.vip != nil {
		if err := d.vip.Release(ctx); err != nil {
			d.logger.Error("failed to release VIP", "error", err)
		} else {
			d.state.SetVIPAcquired(false)
		}
	}

	// Start passive replication & perform an initial catch-up.
	if err := d.passive.Start(ctx); err != nil {
		d.logger.Error("failed to start passive replication", "error", err)
		return
	}

	if err := d.passive.CatchUp(ctx); err != nil {
		d.logger.Error("failed to catch up passive replica", "error", err)
		return
	}

	if lag, err := d.passive.ReplicationLag(ctx); err == nil {
		d.health.SetReplicationLag(uint64(lag.Milliseconds()))
	}
}

// Run starts the daemon and blocks until the context is cancelled or a
// fatal error occurs. It owns the lifetime of the election and health
// checker; backend, VIP, and replication components are managed via
// state callbacks.
func (d *Daemon) Run(ctx context.Context) error {
	d.setCtx(ctx)
	defer d.setCtx(nil)

	d.logger.Info("daemon starting")

	// Start leader election.
	if err := d.election.Start(ctx); err != nil {
		return fmt.Errorf("start election: %w", err)
	}
	defer d.election.Stop()

	// Start health checker.
	if err := d.health.Start(ctx); err != nil {
		return fmt.Errorf("start health checker: %w", err)
	}
	defer d.health.Stop()

	// Initialize leader information.
	leader := d.election.CurrentLeader()
	d.state.SetLeader(leader)
	d.health.SetLeader(leader)

	// Initialize role based on current election state.
	if d.election.IsLeader() {
		if err := d.state.BecomeLeader(); err != nil && err != ErrAlreadyLeader {
			d.logger.Error("failed to transition to leader on startup", "error", err)
		}
	} else {
		// Start passive replication when not leader on startup.
		if err := d.passive.Start(ctx); err != nil {
			d.logger.Error("failed to start passive replication on startup", "error", err)
		}
		d.health.SetRole("PASSIVE")
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("daemon context cancelled, shutting down")

			// Ensure we transition out of PRIMARY state to invoke callbacks.
			if d.state.Role() == RolePrimary {
				if err := d.state.StepDown(); err != nil && err != ErrNotLeader {
					d.logger.Error("failed to step down state on shutdown", "error", err)
				}
			}

			// Final best-effort catch-up when running as passive.
			if !d.election.IsLeader() && d.passive != nil && d.passive.Running() {
				if err := d.passive.CatchUp(context.Background()); err != nil {
					d.logger.Error("final passive catch-up error", "error", err)
				}
			}

			return nil

		case <-d.election.LeaderCh():
			// Election reports that we acquired (or re-acquired) leadership.
			if d.election.IsLeader() {
				if err := d.state.BecomeLeader(); err != nil && err != ErrAlreadyLeader {
					d.logger.Error("failed to transition to leader", "error", err)
				}
			}

		case <-ticker.C:
			// Periodic reconciliation between election state & local state.
			isLeader := d.election.IsLeader()

			if isLeader && d.state.Role() != RolePrimary {
				if err := d.state.BecomeLeader(); err != nil && err != ErrAlreadyLeader {
					d.logger.Error("failed to reconcile leader role", "error", err)
				}
			} else if !isLeader && d.state.Role() == RolePrimary {
				if err := d.state.StepDown(); err != nil && err != ErrNotLeader {
					d.logger.Error("failed to reconcile passive role", "error", err)
				}
			}

			// Keep leader information up to date.
			newLeader := d.election.CurrentLeader()
			if newLeader != d.state.Leader() {
				d.state.SetLeader(newLeader)
			}

			// When not leader, keep the passive replica reasonably up to date
			// and publish replication lag into the health checker.
			if !isLeader {
				if err := d.passive.Start(ctx); err != nil {
					d.logger.Error("failed to start passive replication", "error", err)
				} else {
					if err := d.passive.CatchUp(ctx); err != nil {
						d.logger.Error("passive catch-up error", "error", err)
					} else if lag, err := d.passive.ReplicationLag(ctx); err == nil {
						d.health.SetReplicationLag(uint64(lag.Milliseconds()))
					}
				}
			}
		}
	}
}
