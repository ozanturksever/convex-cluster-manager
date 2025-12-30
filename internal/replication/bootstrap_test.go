package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

func TestBootstrapConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     BootstrapConfig
		wantErr string
	}{
		{
			name:    "empty config",
			cfg:     BootstrapConfig{},
			wantErr: "clusterID is required",
		},
		{
			name: "missing NATS URLs",
			cfg: BootstrapConfig{
				ClusterID: "test-cluster",
			},
			wantErr: "at least one NATS URL is required",
		},
		{
			name: "missing output path",
			cfg: BootstrapConfig{
				ClusterID: "test-cluster",
				NATSURLs:  []string{"nats://localhost:4222"},
			},
			wantErr: "outputPath is required",
		},
		{
			name: "valid config",
			cfg: BootstrapConfig{
				ClusterID:  "test-cluster",
				NATSURLs:   []string{"nats://localhost:4222"},
				OutputPath: "/tmp/test.db",
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestBootstrapConfig_BucketName(t *testing.T) {
	cfg := BootstrapConfig{
		ClusterID: "my-cluster",
	}
	assert.Equal(t, "convex-my-cluster-wal", cfg.BucketName())
}

func TestContainsAny(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substrs  []string
		expected bool
	}{
		{"empty string", "", []string{"foo"}, false},
		{"empty substrs", "hello", []string{}, false},
		{"match first", "no snapshots found", []string{"no snapshots", "error"}, true},
		{"match second", "bucket not found", []string{"no snapshots", "bucket not found"}, true},
		{"no match", "success", []string{"error", "fail"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsAny(tt.s, tt.substrs...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBootstrap_InvalidConfig(t *testing.T) {
	ctx := context.Background()
	err := Bootstrap(ctx, BootstrapConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clusterID is required")
}

func TestBootstrap_NoSnapshotsAvailable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	// Connect and create an empty Object Store bucket.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	clusterID := "test-cluster-bootstrap-empty"
	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Try to bootstrap from an empty bucket.
	outputPath := filepath.Join(t.TempDir(), "bootstrap-empty.db")

	cfg := BootstrapConfig{
		ClusterID:  clusterID,
		NATSURLs:   []string{natsContainer.URL},
		OutputPath: outputPath,
	}

	err = Bootstrap(ctx, cfg)

	// Should return ErrNoSnapshots when no data exists.
	assert.True(t, errors.Is(err, ErrNoSnapshots), "expected ErrNoSnapshots, got: %v", err)
}

func TestBootstrap_RestoresFromSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	// Connect to NATS and create the Object Store bucket.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	clusterID := "test-cluster-bootstrap"
	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a primary SQLite database with some test data.
	primaryDir := t.TempDir()
	primaryDBPath := filepath.Join(primaryDir, "primary.db")

	primaryDB, err := sql.Open("sqlite", primaryDBPath)
	require.NoError(t, err)
	defer primaryDB.Close()

	_, err = primaryDB.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = primaryDB.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`)
	require.NoError(t, err)
	_, err = primaryDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT);`)
	require.NoError(t, err)

	// Insert test data.
	testUsers := []struct {
		name  string
		email string
	}{
		{"Alice", "alice@example.com"},
		{"Bob", "bob@example.com"},
		{"Charlie", "charlie@example.com"},
	}

	for _, user := range testUsers {
		_, err := primaryDB.ExecContext(ctx, `INSERT INTO users (name, email) VALUES (?, ?)`, user.name, user.email)
		require.NoError(t, err)
	}

	// Start the Snapshotter to replicate data to NATS.
	snapCfg := SnapshotConfig{
		Config: Config{
			ClusterID:        clusterID,
			NodeID:           "node-primary",
			NATSURLs:         []string{natsContainer.URL},
			DBPath:           primaryDBPath,
			SnapshotInterval: time.Hour, // Large interval; we'll call snapshotOnce manually
		},
		Retention: time.Hour,
	}

	snap, err := NewSnapshotter(snapCfg)
	require.NoError(t, err)

	require.NoError(t, snap.Start(ctx))
	defer func() { _ = snap.Stop(context.Background()) }()

	// Allow litestream to fully initialize.
	time.Sleep(2 * time.Second)

	// Force a WAL checkpoint to ensure all data is visible.
	_, err = primaryDB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE);`)
	require.NoError(t, err)

	// Take a snapshot manually.
	require.NoError(t, snap.snapshotOnce(ctx))

	// Wait for the snapshot to be uploaded to NATS.
	time.Sleep(2 * time.Second)

	// Stop the snapshotter before bootstrap (ignore cleanup errors).
	_ = snap.Stop(context.Background())

	// Bootstrap to a new location.
	bootstrapDir := t.TempDir()
	bootstrapDBPath := filepath.Join(bootstrapDir, "bootstrap.db")

	bootstrapCfg := BootstrapConfig{
		ClusterID:  clusterID,
		NATSURLs:   []string{natsContainer.URL},
		OutputPath: bootstrapDBPath,
	}

	err = Bootstrap(ctx, bootstrapCfg)
	require.NoError(t, err, "bootstrap should succeed")

	// Verify the bootstrapped database contains the expected data.
	bootstrapDB, err := sql.Open("sqlite", bootstrapDBPath)
	require.NoError(t, err)
	defer bootstrapDB.Close()

	// Count users in the bootstrapped database.
	var bootstrapCount int
	err = bootstrapDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&bootstrapCount)
	require.NoError(t, err)
	assert.Equal(t, len(testUsers), bootstrapCount, "bootstrapped DB should have all users")

	// Verify each user exists with correct data.
	for _, user := range testUsers {
		var name, email string
		err := bootstrapDB.QueryRowContext(ctx, `SELECT name, email FROM users WHERE name = ?`, user.name).Scan(&name, &email)
		require.NoError(t, err, "user %s should exist", user.name)
		assert.Equal(t, user.name, name)
		assert.Equal(t, user.email, email)
	}

	t.Logf("Bootstrap successful: restored %d users to %s", bootstrapCount, bootstrapDBPath)
}

func TestBootstrap_RestoresWithPrimaryReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	// Connect to NATS and create the Object Store bucket.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	clusterID := "test-cluster-bootstrap-primary"
	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a primary SQLite database with some test data.
	primaryDir := t.TempDir()
	primaryDBPath := filepath.Join(primaryDir, "primary.db")

	primaryDB, err := sql.Open("sqlite", primaryDBPath)
	require.NoError(t, err)
	defer primaryDB.Close()

	_, err = primaryDB.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = primaryDB.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`)
	require.NoError(t, err)
	_, err = primaryDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT, created_at TEXT);`)
	require.NoError(t, err)

	// Start Primary replication to NATS.
	primaryCfg := Config{
		ClusterID: clusterID,
		NodeID:    "node-primary",
		NATSURLs:  []string{natsContainer.URL},
		DBPath:    primaryDBPath,
	}

	primary, err := NewPrimary(primaryCfg)
	require.NoError(t, err)

	require.NoError(t, primary.Start(ctx))
	defer func() { _ = primary.Stop(context.Background()) }()

	// Allow litestream to fully initialize.
	time.Sleep(2 * time.Second)

	// Insert test data while replication is running.
	for i := 0; i < 10; i++ {
		_, err := primaryDB.ExecContext(ctx,
			`INSERT INTO events (message, created_at) VALUES (?, datetime('now'))`,
			fmt.Sprintf("event-%d", i))
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
	}

	// Force a WAL checkpoint.
	_, err = primaryDB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE);`)
	require.NoError(t, err)

	// Wait for litestream to sync changes to NATS.
	time.Sleep(3 * time.Second)

	// Stop primary before bootstrap (ignore cleanup errors).
	_ = primary.Stop(context.Background())

	// Bootstrap to a new location.
	bootstrapDir := t.TempDir()
	bootstrapDBPath := filepath.Join(bootstrapDir, "bootstrap.db")

	bootstrapCfg := BootstrapConfig{
		ClusterID:  clusterID,
		NATSURLs:   []string{natsContainer.URL},
		OutputPath: bootstrapDBPath,
	}

	err = Bootstrap(ctx, bootstrapCfg)
	require.NoError(t, err, "bootstrap should succeed")

	// Verify the bootstrapped database contains the expected data.
	bootstrapDB, err := sql.Open("sqlite", bootstrapDBPath)
	require.NoError(t, err)
	defer bootstrapDB.Close()

	// Get counts from both databases.
	var primaryCount, bootstrapCount int
	err = primaryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&primaryCount)
	require.NoError(t, err)
	err = bootstrapDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&bootstrapCount)
	require.NoError(t, err)

	assert.Equal(t, primaryCount, bootstrapCount, "bootstrapped DB should have same event count as primary")
	assert.Equal(t, 10, bootstrapCount, "bootstrapped DB should have all 10 events")

	t.Logf("Bootstrap successful: restored %d events from primary to %s", bootstrapCount, bootstrapDBPath)
}
