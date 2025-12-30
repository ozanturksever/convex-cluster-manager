package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	litestreamnats "github.com/benbjohnson/litestream/nats"
)

// SnapshotConfig configures the snapshot manager.
//
// It embeds the replication Config used by the primary/passive replicators
// and adds a Retention field which should typically be set from
// cfg.WAL.StreamRetention in the cluster configuration.
type SnapshotConfig struct {
	Config

	// Retention controls how long snapshots are kept before being
	// deleted. Older snapshots are removed via litestream's
	// EnforceSnapshotRetention, using this duration as a cutoff.
	Retention time.Duration
}

// Validate validates the snapshot configuration.
func (c SnapshotConfig) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.SnapshotInterval <= 0 {
		return fmt.Errorf("snapshotInterval must be > 0")
	}
	if c.Retention <= 0 {
		return fmt.Errorf("retention must be > 0")
	}
	return nil
}

// Snapshotter manages periodic litestream snapshots of a SQLite database
// to a NATS JetStream Object Store bucket and enforces time-based
// retention on those snapshots.
//
// Bucket naming follows the same convention as the primary/passive
// replicators: convex.<clusterID>.wal.
type Snapshotter struct {
	cfg    SnapshotConfig
	logger *slog.Logger

	mu      sync.Mutex
	running bool

	db      *litestream.DB
	replica *litestream.Replica
	client  *litestreamnats.ReplicaClient

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSnapshotter creates a new Snapshotter with the given configuration.
func NewSnapshotter(cfg SnapshotConfig) (*Snapshotter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	s := &Snapshotter{
		cfg: cfg,
		logger: slog.Default().With(
			"component", "replication-snapshot",
			"cluster", cfg.ClusterID,
			"node", cfg.NodeID,
		),
	}
	return s, nil
}

// BucketName returns the NATS Object Store bucket name used for
// snapshots for this cluster.
func (s *Snapshotter) BucketName() string {
	return fmt.Sprintf("convex-%s-wal", s.cfg.ClusterID)
}

// Start initializes litestream for the configured database and begins a
// background goroutine that periodically writes snapshots and enforces
// retention.
//
// Calling Start multiple times is safe; subsequent calls are ignored
// once the snapshotter is running.
func (s *Snapshotter) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	// Create litestream DB wrapper for the target database.
	db := litestream.NewDB(s.cfg.DBPath)

	// Configure NATS replica client used as an Object Store backend.
	client := litestreamnats.NewReplicaClient()
	client.URL = s.cfg.NATSURLs[0]
	client.BucketName = s.BucketName()
	client.Path = s.cfg.ReplicaPath
	if s.cfg.NATSCredentials != "" {
		client.Creds = s.cfg.NATSCredentials
	}

	// Attach replica to DB.
	replica := litestream.NewReplicaWithClient(db, client)
	db.Replica = replica

	// Open DB monitoring so that litestream can manage the WAL shadow
	// state. This also starts the internal sync monitor.
	if err := db.Open(); err != nil {
		_ = client.Close()
		return fmt.Errorf("open litestream db: %w", err)
	}

	s.db = db
	s.replica = replica
	s.client = client

	// Start background snapshot loop.
	loopCtx, cancel := context.WithCancel(ctx)
	s.ctx = loopCtx
	s.cancel = cancel
	s.running = true

	s.wg.Add(1)
	go s.loop()

	s.logger.Info("snapshotter started",
		"db", s.cfg.DBPath,
		"bucket", client.BucketName,
		"nats", s.cfg.NATSURLs[0],
		"interval", s.cfg.SnapshotInterval,
		"retention", s.cfg.Retention,
	)

	return nil
}

// Stop stops the background snapshot loop and closes litestream
// resources. It is safe to call Stop even if Start was never called.
func (s *Snapshotter) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}

	cancel := s.cancel
	db := s.db
	client := s.client

	s.cancel = nil
	s.ctx = nil
	s.db = nil
	s.replica = nil
	s.client = nil
	s.running = false
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	s.wg.Wait()

	var err error
	if db != nil {
		if e := db.Close(ctx); e != nil {
			err = fmt.Errorf("close litestream db: %w", e)
		}
	}
	if client != nil {
		if e := client.Close(); e != nil && err == nil {
			err = fmt.Errorf("close nats replica client: %w", e)
		}
	}

	s.logger.Info("snapshotter stopped")
	return err
}

// Running reports whether the snapshotter is currently active.
func (s *Snapshotter) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// loop runs in a background goroutine and periodically takes
// snapshots and enforces retention.
func (s *Snapshotter) loop() {
	defer s.wg.Done()

	// Capture ctx at start to avoid race with Stop() setting s.ctx = nil
	s.mu.Lock()
	ctx := s.ctx
	s.mu.Unlock()

	if ctx == nil {
		return
	}

	ticker := time.NewTicker(s.cfg.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.snapshotOnce(ctx); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				s.logger.Error("snapshot error", "error", err)
			}
		}
	}
}

// snapshotOnce performs a single snapshot and retention pass.
//
// It is primarily used internally by the snapshot loop but is also
// exposed for tests.
func (s *Snapshotter) snapshotOnce(ctx context.Context) error {
	s.mu.Lock()
	db := s.db
	running := s.running
	s.mu.Unlock()

	if !running || db == nil {
		return fmt.Errorf("snapshotter not started")
	}

	// Create a new snapshot at the current database position.
	info, err := db.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	s.logger.Debug("snapshot created",
		"size", info.Size,
		"createdAt", info.CreatedAt,
	)

	// Enforce time-based retention on snapshot level using the
	// configured retention window.
	threshold := time.Now().Add(-s.cfg.Retention)
	if _, err := db.EnforceSnapshotRetention(ctx, threshold); err != nil {
		return fmt.Errorf("enforce snapshot retention: %w", err)
	}

	return nil
}
