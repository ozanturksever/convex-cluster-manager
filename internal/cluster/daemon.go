package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ozanturksever/convex-cluster-manager/internal/backend"
	"github.com/ozanturksever/convex-cluster-manager/internal/config"
	"github.com/ozanturksever/convex-cluster-manager/internal/health"
	"github.com/ozanturksever/convex-cluster-manager/internal/replication"
	"github.com/ozanturksever/convex-cluster-manager/internal/vip"

	_ "modernc.org/sqlite" // Pure Go SQLite driver for WAL mode verification (no CGO required)
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
//
// IMPORTANT: Before starting the backend, this function promotes the replica
// database to the data path if the replica contains newer data. This ensures
// data written to a former passive node (that became primary) is preserved
// when the original primary returns.
func (d *Daemon) handleBecomeLeader() {
	ctx := d.getCtx()

	d.logger.Info("becoming PRIMARY")
	d.health.SetRole("PRIMARY")
	d.health.SetLeader(d.cfg.NodeID)

	// Perform a final catch-up on the replica before stopping passive replication.
	// This ensures we have the latest data from the previous primary.
	// We use a dedicated context with timeout to avoid being cancelled prematurely.
	// We also retry multiple times to handle transient failures.
	catchUpSucceeded := false
	if d.passive != nil && d.passive.Running() {
		d.logger.Info("performing final catch-up before promotion")
		
		// Use a dedicated context with timeout for the final catch-up.
		// This ensures we don't get cancelled by the parent context during promotion.
		const catchUpTimeout = 30 * time.Second
		const maxRetries = 3
		
		for attempt := 1; attempt <= maxRetries; attempt++ {
			catchUpCtx, cancelCatchUp := context.WithTimeout(context.Background(), catchUpTimeout)
			err := d.passive.CatchUp(catchUpCtx)
			cancelCatchUp()
			
			if err == nil {
				d.logger.Info("final catch-up succeeded", "attempt", attempt)
				catchUpSucceeded = true
				break
			}
			
			d.logger.Warn("final catch-up attempt failed", 
				"attempt", attempt, 
				"maxRetries", maxRetries, 
				"error", err)
			
			if attempt < maxRetries {
				// Brief pause before retry
				time.Sleep(time.Second)
			}
		}
		
		if !catchUpSucceeded {
			d.logger.Warn("all catch-up attempts failed, will NOT promote replica to data path")
		}
	}

	// Stop passive replication if running.
	if d.passive != nil {
		// Use a dedicated context for stopping to avoid cancellation issues
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 10*time.Second)
		if err := d.passive.Stop(stopCtx); err != nil {
			d.logger.Error("failed to stop passive replication", "error", err)
		}
		cancelStop()
	}

	// Promote the replica database to the data path ONLY if catch-up succeeded.
	// This is critical for data integrity - if catch-up failed, the replica may
	// be stale and promoting it would overwrite newer data on the data path.
	// In that case, we let the backend start with whatever data is available
	// on the data path (which may be more recent than a stale replica).
	if catchUpSucceeded {
		if err := d.promoteReplicaToData(ctx); err != nil {
			d.logger.Error("failed to promote replica to data path", "error", err)
			// Continue anyway - the backend will use whatever data is available
		}
	} else {
		d.logger.Warn("skipping replica promotion due to failed catch-up - using existing data path")
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

	// Start primary WAL replication, WAL mode verification, and snapshotter in a single goroutine.
	// This consolidates all database-dependent initialization that needs to wait for the
	// backend to create the database file.
	go d.startPrimaryReplicationWithRetry(ctx)
}

// startPrimaryReplicationWithRetry waits for the database file to exist
// and then performs all database-dependent initialization:
// 1. Verifies WAL mode is enabled
// 2. Starts primary WAL replication
// 3. Starts periodic snapshots
//
// This consolidates all initialization that depends on the database file existing,
// avoiding duplicate polling loops and potential race conditions.
func (d *Daemon) startPrimaryReplicationWithRetry(ctx context.Context) {
	dbPath := d.cfg.Backend.DataPath
	if dbPath == "" {
		d.logger.Warn("no data path configured, skipping primary replication")
		return
	}

	// Wait for database file to exist (max 60 seconds)
	dbExists := false
	for i := 0; i < 60; i++ {
		select {
		case <-ctx.Done():
			d.logger.Info("context cancelled while waiting for database file")
			return
		default:
		}

		if _, err := os.Stat(dbPath); err == nil {
			dbExists = true
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !dbExists {
		d.logger.Warn("database file did not appear within timeout", "path", dbPath)
		return
	}

	// Wait a bit more for backend to fully initialize the DB
	time.Sleep(2 * time.Second)

	// Check if context was cancelled during the wait
	select {
	case <-ctx.Done():
		return
	default:
	}

	// Step 1: Verify WAL mode is enabled
	if err := verifyWALMode(dbPath); err != nil {
		d.logger.Error("WAL mode verification failed - replication will not work correctly", "error", err)
		d.health.SetBackendStatus("running-no-wal")
		// Continue anyway - replication might still work if WAL mode is enabled later
	} else {
		d.logger.Info("WAL mode verified for database", "path", dbPath)
	}

	// Step 2: Start primary WAL replication
	if err := d.primary.Start(ctx); err != nil {
		d.logger.Error("failed to start primary replication", "error", err)
	} else {
		d.logger.Info("primary replication started successfully")
	}

	// Step 3: Start periodic snapshots
	if err := d.snapshotter.Start(ctx); err != nil {
		d.logger.Error("failed to start snapshotter", "error", err)
	}
}

// handleStepDown is invoked when this node transitions from PRIMARY to
// PASSIVE. It stops primary-specific components, releases the VIP, and
// starts passive replication and an initial catch-up.
func (d *Daemon) handleStepDown() {
	// Use a dedicated context for step-down operations to avoid cancellation issues.
	// The parent context may be cancelled during shutdown, but we still want to
	// complete the step-down cleanly.
	stepDownCtx, cancelStepDown := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelStepDown()

	d.logger.Info("stepping down to PASSIVE")
	d.health.SetRole("PASSIVE")

	// Stop snapshotter first so no new snapshots are taken.
	if err := d.snapshotter.Stop(stepDownCtx); err != nil {
		d.logger.Error("failed to stop snapshotter", "error", err)
	}

	// Stop primary replication.
	if err := d.primary.Stop(stepDownCtx); err != nil {
		d.logger.Error("failed to stop primary replication", "error", err)
	}

	// Stop backend service.
	if err := d.backend.Stop(stepDownCtx); err != nil {
		d.logger.Error("failed to stop backend service", "error", err)
	}
	d.health.SetBackendStatus("stopped")

	// Release VIP.
	if d.vip != nil {
		if err := d.vip.Release(stepDownCtx); err != nil {
			d.logger.Error("failed to release VIP", "error", err)
		} else {
			d.state.SetVIPAcquired(false)
		}
	}

	// Start passive replication & perform an initial catch-up.
	if err := d.passive.Start(stepDownCtx); err != nil {
		d.logger.Error("failed to start passive replication", "error", err)
		return
	}

	if err := d.passive.CatchUp(stepDownCtx); err != nil {
		d.logger.Error("failed to catch up passive replica", "error", err)
		return
	}

	if lag, err := d.passive.ReplicationLag(stepDownCtx); err == nil {
		d.health.SetReplicationLag(uint64(lag.Milliseconds()))
	}
}

// promoteReplicaToData copies the replica database to the data path if the
// replica contains newer data. This is essential for data integrity during
// failover scenarios where the passive node has received WAL updates from
// the former primary.
//
// The function compares the transaction IDs (via page count as a proxy) of
// both databases and only copies if the replica is newer.
func (d *Daemon) promoteReplicaToData(_ context.Context) error {
	replicaPath := d.cfg.WAL.ReplicaPath
	dataPath := d.cfg.Backend.DataPath

	// Check if replica exists
	replicaInfo, err := os.Stat(replicaPath)
	if os.IsNotExist(err) {
		d.logger.Debug("no replica database to promote", "path", replicaPath)
		return nil
	} else if err != nil {
		return fmt.Errorf("stat replica: %w", err)
	}

	// Check if data path exists
	dataInfo, dataErr := os.Stat(dataPath)
	dataExists := dataErr == nil

	// Compare databases to determine if replica is newer
	replicaNewer := false

	if !dataExists {
		// No data file exists, replica is definitely newer
		replicaNewer = true
		d.logger.Info("data path does not exist, will use replica", "replica", replicaPath, "data", dataPath)
	} else {
		// Both exist - try TXID comparison first (most accurate), fall back to mtime
		replicaTxID, err1 := d.getDBTxID(replicaPath)
		dataTxID, err2 := d.getDBTxID(dataPath)

		if err1 == nil && err2 == nil {
			// TXID comparison is authoritative
			if replicaTxID > dataTxID {
				replicaNewer = true
				d.logger.Info("replica has higher transaction ID",
					"replica_txid", replicaTxID,
					"data_txid", dataTxID)
			} else {
				d.logger.Info("data path is up to date, no promotion needed",
					"replica_txid", replicaTxID,
					"data_txid", dataTxID)
			}
		} else {
			// Fall back to mtime comparison if we can't read TXIDs
			d.logger.Warn("could not compare database TXIDs, falling back to mtime",
				"replica_err", err1,
				"data_err", err2)
			if replicaInfo.ModTime().After(dataInfo.ModTime()) {
				replicaNewer = true
				d.logger.Info("replica is newer than data (by mtime)",
					"replica_mtime", replicaInfo.ModTime(),
					"data_mtime", dataInfo.ModTime())
			}
		}
	}

	if !replicaNewer {
		d.logger.Debug("replica is not newer, skipping promotion")
		return nil
	}

	// Promote: copy replica to data path atomically
	d.logger.Info("promoting replica to data path",
		"replica", replicaPath,
		"data", dataPath)

	// Ensure data directory exists
	dataDir := filepath.Dir(dataPath)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	// Copy to temp file first, then rename for atomicity
	tmpPath := dataPath + ".promoting.tmp"

	// Remove any leftover temp files
	_ = os.Remove(tmpPath)
	_ = os.Remove(tmpPath + "-wal")
	_ = os.Remove(tmpPath + "-shm")

	// Copy replica to temp location
	if err := copyFile(replicaPath, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("copy replica to temp: %w", err)
	}

	// Remove old data files (including WAL and SHM)
	_ = os.Remove(dataPath + "-wal")
	_ = os.Remove(dataPath + "-shm")
	_ = os.Remove(dataPath)

	// Atomically rename temp to data path
	if err := os.Rename(tmpPath, dataPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp to data: %w", err)
	}

	d.logger.Info("replica promoted to data path successfully")
	return nil
}

// getDBTxID returns the current transaction ID from a SQLite database.
// It uses the data_version pragma which reflects the schema cookie + data changes.
func (d *Daemon) getDBTxID(dbPath string) (int64, error) {
	// Open read-only with busy timeout to handle locks gracefully
	db, err := sql.Open("sqlite", dbPath+"?mode=ro&_busy_timeout=1000")
	if err != nil {
		return 0, fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Use page_count as a proxy for transaction progress
	// Higher page count generally means more data/transactions
	var pageCount int64
	if err := db.QueryRow("PRAGMA page_count;").Scan(&pageCount); err != nil {
		return 0, fmt.Errorf("query page_count: %w", err)
	}

	// Also get data_version for additional comparison
	var dataVersion int64
	if err := db.QueryRow("PRAGMA data_version;").Scan(&dataVersion); err != nil {
		// data_version might not be available, just use page_count
		return pageCount, nil
	}

	// Combine page_count and data_version for a more accurate comparison
	// page_count is the primary indicator, data_version breaks ties
	return pageCount*1000000 + dataVersion, nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	// Ensure data is flushed to disk
	return dstFile.Sync()
}

// verifyWALMode checks if the database at the given path is in WAL mode.
// Returns nil if WAL mode is enabled, an error otherwise.
func verifyWALMode(dbPath string) error {
	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		// Database doesn't exist yet - it will be created by the backend
		// We'll verify WAL mode after the backend starts
		return nil
	}

	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return fmt.Errorf("failed to open database for WAL verification: %w", err)
	}
	defer db.Close()

	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode)
	if err != nil {
		return fmt.Errorf("failed to query journal mode: %w", err)
	}

	if strings.ToLower(journalMode) != "wal" {
		return fmt.Errorf("database is not in WAL mode (current: %s). "+
			"The cluster manager requires the convex-backend fork with WAL support. "+
			"See README.md for details", journalMode)
	}

	return nil
}

// Run starts the daemon and blocks until the context is cancelled or a
// fatal error occurs. It owns the lifetime of the election and health
// checker; backend, VIP, and replication components are managed via
// state callbacks.
func (d *Daemon) Run(ctx context.Context) error {
	d.setCtx(ctx)
	defer d.setCtx(context.Background())

	d.logger.Info("daemon starting")

	// Verify WAL mode if database exists
	if d.cfg.Backend.DataPath != "" {
		if err := verifyWALMode(d.cfg.Backend.DataPath); err != nil {
			d.logger.Warn("WAL mode verification", "status", "database not yet created or verification skipped", "path", d.cfg.Backend.DataPath)
		}
	}

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
			// We check both IsLeader() (authoritative) AND CurrentLeader() == nodeID
			// (secondary check) to handle race conditions where the election state
			// might be temporarily inconsistent.
			isLeader := d.election.IsLeader()
			currentLeader := d.election.CurrentLeader()
			
			// This node should be PRIMARY if:
			// 1. election.IsLeader() returns true (authoritative), OR
			// 2. currentLeader == this node's ID (the KV store says we're leader)
			//    This handles the case where IsLeader() might be false due to a
			//    temporary state inconsistency (e.g., after failed renewal).
			shouldBePrimary := isLeader || currentLeader == d.cfg.NodeID

			if shouldBePrimary && d.state.Role() != RolePrimary {
				d.logger.Info("reconciling to PRIMARY", 
					"isLeader", isLeader, 
					"currentLeader", currentLeader,
					"currentRole", d.state.Role())
				if err := d.state.BecomeLeader(); err != nil && err != ErrAlreadyLeader {
					d.logger.Error("failed to reconcile leader role", "error", err)
				}
			} else if !shouldBePrimary && d.state.Role() == RolePrimary {
				d.logger.Info("reconciling to PASSIVE",
					"isLeader", isLeader,
					"currentLeader", currentLeader,
					"currentRole", d.state.Role())
				if err := d.state.StepDown(); err != nil && err != ErrNotLeader {
					d.logger.Error("failed to reconcile passive role", "error", err)
				}
			}

			// Keep leader information up to date in state.
			// Note: This doesn't affect role - role is determined by shouldBePrimary above.
			if currentLeader != d.state.Leader() {
				d.state.SetLeader(currentLeader)
			}

			// When leader, ensure primary replication is running.
			// This handles cases where replication failed to start initially
			// (e.g., database didn't exist yet) or stopped unexpectedly.
			if isLeader {
				if !d.primary.Running() {
					d.logger.Info("primary replication not running, attempting to start")
					if err := d.primary.Start(ctx); err != nil {
						d.logger.Error("failed to start primary replication", "error", err)
					} else {
						d.logger.Info("primary replication started successfully")
					}
				}
				// Also ensure snapshotter is running
				if !d.snapshotter.Running() {
					if err := d.snapshotter.Start(ctx); err != nil {
						d.logger.Error("failed to start snapshotter", "error", err)
					}
				}
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
