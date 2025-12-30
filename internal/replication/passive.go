package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	litestreamnats "github.com/benbjohnson/litestream/nats"
)

// Passive manages WAL replication for a passive node by consuming LTX
// files from a NATS JetStream Object Store bucket and restoring them
// into a local shadow database.
//
// The Config type is shared with the primary replicator. For a passive
// node, DBPath should be set to the shadow database path (typically
// cfg.WAL.ReplicaPath from the cluster configuration).
type Passive struct {
	cfg    Config
	logger *slog.Logger

	mu          sync.Mutex
	running     bool
	client      *litestreamnats.ReplicaClient
	replica     *litestream.Replica
	lastUpdated time.Time
}

// NewPassive creates a new Passive replicator with the given configuration.
func NewPassive(cfg Config) (*Passive, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	p := &Passive{
		cfg: cfg,
		logger: slog.Default().With(
			"component", "replication-passive",
			"cluster", cfg.ClusterID,
			"node", cfg.NodeID,
		),
	}
	return p, nil
}

// BucketName returns the NATS Object Store bucket name used for WAL
// replication for this cluster.
func (p *Passive) BucketName() string {
	return fmt.Sprintf("convex-%s-wal", p.cfg.ClusterID)
}

// Start initializes the replica client for the configured NATS Object
// Store bucket. It does not perform any restore by itself; callers
// should invoke CatchUp to apply changes to the local shadow database.
//
// Start is idempotent.
func (p *Passive) Start(ctx context.Context) error { // ctx is reserved for future use
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return nil
	}

	if len(p.cfg.NATSURLs) == 0 {
		return fmt.Errorf("at least one NATS URL is required")
	}

	// Ensure the replica database directory exists.
	dbDir := filepath.Dir(p.cfg.DBPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("create replica directory: %w", err)
	}

	client := litestreamnats.NewReplicaClient()
	client.URL = p.cfg.NATSURLs[0]
	client.BucketName = p.BucketName()
	client.Path = p.cfg.ReplicaPath
	if p.cfg.NATSCredentials != "" {
		client.Creds = p.cfg.NATSCredentials
	}

	// Replica is created without an attached DB because we only use it
	// for Restore() into the configured output path.
	replica := litestream.NewReplica(nil)
	replica.Client = client

	p.client = client
	p.replica = replica
	p.running = true

	p.logger.Info("passive replication started",
		"db", p.cfg.DBPath,
		"bucket", client.BucketName,
		"nats", p.cfg.NATSURLs[0],
	)

	return nil
}

// Stop closes the replica client and marks the passive replicator as
// stopped. It is safe to call Stop even if Start was never called.
func (p *Passive) Stop(ctx context.Context) error { // ctx reserved for symmetry with Start
	p.mu.Lock()
	client := p.client
	p.client = nil
	p.replica = nil
	p.running = false
	p.mu.Unlock()

	var err error
	if client != nil {
		if e := client.Close(); e != nil {
			err = fmt.Errorf("close nats replica client: %w", e)
		}
	}

	p.logger.Info("passive replication stopped")
	return err
}

// Running reports whether the passive replicator has been started.
func (p *Passive) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// CatchUp performs a one-shot restore from the remote replica into the
// local shadow database at cfg.DBPath. It uses litestream's Restore
// planning to choose the appropriate snapshot and WAL files, then
// atomically replaces the local database file.
//
// If no snapshots are available yet (e.g., new cluster or primary hasn't
// synced yet), CatchUp returns nil without error - the passive node will
// retry on the next catch-up cycle.
func (p *Passive) CatchUp(ctx context.Context) error {
	p.mu.Lock()
	replica := p.replica
	client := p.client
	running := p.running
	p.mu.Unlock()

	if !running {
		return fmt.Errorf("passive replication not started")
	}
	if replica == nil || client == nil {
		return fmt.Errorf("replica not initialized")
	}

	// Initialize the NATS client connection before attempting restore.
	// This ensures the connection is established and object store is accessible.
	if err := client.Init(ctx); err != nil {
		// Check if this is a "no bucket" or similar situation - not an error for passive.
		if isPassiveNoSnapshotsError(err) {
			p.logger.Debug("NATS object store not ready yet", "error", err)
			return nil
		}
		return fmt.Errorf("init nats client: %w", err)
	}

	// Restore to a temporary file first, then atomically move into place.
	// This avoids the "output path already exists" error from litestream
	// and ensures atomic updates to the shadow database.
	tmpPath := p.cfg.DBPath + ".tmp"

	// Clean up any leftover temp file from a previous failed restore.
	_ = os.Remove(tmpPath)
	_ = os.Remove(tmpPath + "-wal")
	_ = os.Remove(tmpPath + "-shm")

	opt := litestream.NewRestoreOptions()
	opt.OutputPath = tmpPath

	updatedAt, err := replica.CalcRestoreTarget(ctx, opt)
	if err != nil {
		// Check if this is a "no snapshots" situation - not an error for passive.
		if isPassiveNoSnapshotsError(err) {
			p.logger.Debug("no snapshots available yet for catch-up")
			return nil
		}
		return fmt.Errorf("calc restore target: %w", err)
	}

	// If updatedAt is zero, there's nothing to restore yet.
	if updatedAt.IsZero() {
		p.logger.Debug("no restore target available yet")
		return nil
	}

	if err := replica.Restore(ctx, opt); err != nil {
		// Clean up failed temp file.
		_ = os.Remove(tmpPath)
		_ = os.Remove(tmpPath + "-wal")
		_ = os.Remove(tmpPath + "-shm")
		
		// Check for no snapshots error during restore.
		if isPassiveNoSnapshotsError(err) || errors.Is(err, litestream.ErrNoSnapshots) {
			p.logger.Debug("no snapshots available for restore")
			return nil
		}
		return fmt.Errorf("restore from replica: %w", err)
	}

	// Ensure output directory exists.
	outputDir := filepath.Dir(p.cfg.DBPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("create output directory: %w", err)
	}

	// Remove existing database files before moving the new one into place.
	// This is safe because the passive node doesn't serve traffic.
	_ = os.Remove(p.cfg.DBPath + "-wal")
	_ = os.Remove(p.cfg.DBPath + "-shm")
	_ = os.Remove(p.cfg.DBPath)

	// Atomically move the temp file into place.
	if err := os.Rename(tmpPath, p.cfg.DBPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp db to final path: %w", err)
	}

	p.mu.Lock()
	p.lastUpdated = updatedAt
	p.mu.Unlock()

	p.logger.Info("passive replica caught up", "updatedAt", updatedAt)

	return nil
}

// ReplicationLag returns how far behind the passive node is relative to
// the remote replica, in terms of wall-clock time between the latest
// available LTX data and the last successfully applied restore.
//
// A lag of zero indicates that the last restore was performed at the
// most recent available point in the replica. If no restore has been
// performed yet, the lag is reported as zero.
//
// If the replica has no data yet (e.g., new cluster), returns zero with no error.
func (p *Passive) ReplicationLag(ctx context.Context) (time.Duration, error) {
	p.mu.Lock()
	replica := p.replica
	client := p.client
	last := p.lastUpdated
	running := p.running
	p.mu.Unlock()

	if !running || replica == nil || client == nil {
		return 0, fmt.Errorf("passive replication not started")
	}

	// Initialize the NATS client connection before querying.
	if err := client.Init(ctx); err != nil {
		// If no bucket available, report zero lag.
		if isPassiveNoSnapshotsError(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("init nats client: %w", err)
	}

	opt := litestream.NewRestoreOptions()
	updatedAt, err := replica.CalcRestoreTarget(ctx, opt)
	if err != nil {
		// If no snapshots available, report zero lag (nothing to catch up to).
		if isPassiveNoSnapshotsError(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("calc restore target: %w", err)
	}

	// If no data available yet, report zero lag.
	if updatedAt.IsZero() {
		return 0, nil
	}

	if last.IsZero() {
		return 0, nil
	}
	if !updatedAt.After(last) {
		return 0, nil
	}

	return updatedAt.Sub(last), nil
}

// isPassiveNoSnapshotsError checks if the error indicates no snapshots are available.
// This is a local helper since the passive replicator needs to handle this case gracefully.
func isPassiveNoSnapshotsError(err error) bool {
	if err == nil {
		return false
	}
	// Litestream returns various errors when no snapshots exist.
	// Check for common patterns.
	errStr := err.Error()
	if errors.Is(err, litestream.ErrNoSnapshots) {
		return true
	}
	noSnapshotPatterns := []string{"no snapshots", "no generations", "bucket not found", "key not found", "no matching backup"}
	for _, pattern := range noSnapshotPatterns {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}
	return false
}
