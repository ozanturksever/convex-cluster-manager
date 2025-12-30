package replication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	litestreamnats "github.com/benbjohnson/litestream/nats"
)

// Config configures the primary WAL replication process.
//
// DBPath should point to the SQLite database used by the Convex backend
// (typically cfg.Backend.DataPath). NATSURLs should come from cfg.NATS.Servers.
// ClusterID and NodeID are used for logging and bucket naming.
type Config struct {
	// Cluster identifier, used in the NATS Object Store bucket name.
	ClusterID string

	// Node identifier, used only for logging at this layer.
	NodeID string

	// NATS connection settings. Only the first URL is used for now.
	NATSURLs []string

	// Optional path to a NATS credentials file (for user JWT auth).
	NATSCredentials string

	// Path to the primary SQLite database file (backend data path).
	DBPath string

	// Optional snapshot interval override. If zero, litestream defaults are used.
	SnapshotInterval time.Duration

	// Optional logical path prefix within the Object Store bucket for this DB.
	// If empty, LTX files are written at the bucket root.
	ReplicaPath string
}

// Validate validates the primary replication configuration.
func (c Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("clusterID is required")
	}
	if c.NodeID == "" {
		return fmt.Errorf("nodeID is required")
	}
	if len(c.NATSURLs) == 0 {
		return fmt.Errorf("at least one NATS URL is required")
	}
	if c.DBPath == "" {
		return fmt.Errorf("dbPath is required")
	}
	return nil
}

// Primary manages WAL replication for the primary node using litestream
// and a NATS Object Store replica client.
//
// A Primary is intended to be started when this node becomes leader and
// stopped when it steps down.
type Primary struct {
	cfg    Config
	logger *slog.Logger

	mu      sync.Mutex
	running bool

	db      *litestream.DB
	store   *litestream.Store
	replica *litestream.Replica
	client  *litestreamnats.ReplicaClient
}

// NewPrimary creates a new Primary replicator with the given configuration.
func NewPrimary(cfg Config) (*Primary, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	p := &Primary{
		cfg: cfg,
		logger: slog.Default().With(
			"component", "replication-primary",
			"cluster", cfg.ClusterID,
			"node", cfg.NodeID,
		),
	}
	return p, nil
}

// BucketName returns the NATS Object Store bucket name used for WAL
// replication for this cluster.
func (p *Primary) BucketName() string {
	return fmt.Sprintf("convex-%s-wal", p.cfg.ClusterID)
}

// Start initializes litestream for the configured database and begins
// background WAL replication to NATS. It is safe to call Start multiple times.
func (p *Primary) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return nil
	}

	// Ensure the database directory exists.
	dbDir := filepath.Dir(p.cfg.DBPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}

	// Check if database file exists - if not, we can't start replication yet.
	if _, err := os.Stat(p.cfg.DBPath); os.IsNotExist(err) {
		p.logger.Info("database file does not exist yet, skipping primary replication start")
		return nil
	}

	// Create litestream DB wrapper for the backend database.
	db := litestream.NewDB(p.cfg.DBPath)

	// Configure NATS replica client.
	client := litestreamnats.NewReplicaClient()
	client.URL = p.cfg.NATSURLs[0]
	client.BucketName = p.BucketName()
	client.Path = p.cfg.ReplicaPath
	if p.cfg.NATSCredentials != "" {
		client.Creds = p.cfg.NATSCredentials
	}

	// Wire replica to DB.
	replica := litestream.NewReplicaWithClient(db, client)
	db.Replica = replica

	// Configure simple two-level compaction plus snapshots.
	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: 10 * time.Second},
	}

	store := litestream.NewStore([]*litestream.DB{db}, levels)
	if p.cfg.SnapshotInterval > 0 {
		store.SnapshotInterval = p.cfg.SnapshotInterval
	}

	if err := store.Open(ctx); err != nil {
		// Best-effort cleanup; db/replica will be closed by store if partially opened.
		_ = client.Close()
		return fmt.Errorf("open litestream store: %w", err)
	}

	p.db = db
	p.store = store
	p.replica = replica
	p.client = client
	p.running = true

	p.logger.Info("primary replication started",
		"db", p.cfg.DBPath,
		"bucket", client.BucketName,
		"nats", p.cfg.NATSURLs[0],
	)

	return nil
}

// Stop stops WAL replication and closes litestream resources. It is safe to
// call Stop even if Start was never called.
func (p *Primary) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}

	store := p.store
	db := p.db
	client := p.client

	p.store = nil
	p.db = nil
	p.replica = nil
	p.client = nil
	p.running = false
	p.mu.Unlock()

	var err error
	if store != nil {
		if e := store.Close(ctx); e != nil {
			err = fmt.Errorf("close litestream store: %w", e)
		}
	} else if db != nil {
		if e := db.Close(ctx); e != nil && err == nil {
			err = fmt.Errorf("close litestream db: %w", e)
		}
	}

	if client != nil {
		if e := client.Close(); e != nil && err == nil {
			err = fmt.Errorf("close nats replica client: %w", e)
		}
	}

	p.logger.Info("primary replication stopped")
	return err
}

// Running reports whether the primary replicator is currently active.
func (p *Primary) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}
