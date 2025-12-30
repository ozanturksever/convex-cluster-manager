package replication

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

func TestSnapshotConfigValidation(t *testing.T) {
	t.Run("requires clusterID", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				NodeID:          "node-1",
				NATSURLs:        []string{"nats://localhost:4222"},
				DBPath:          "/tmp/db.sqlite",
				SnapshotInterval: time.Minute,
			},
			Retention: time.Hour,
		}
		_, err := NewSnapshotter(cfg)
		assert.Error(t, err)
	})

	t.Run("requires nodeID", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				ClusterID:       "cluster-1",
				NATSURLs:        []string{"nats://localhost:4222"},
				DBPath:          "/tmp/db.sqlite",
				SnapshotInterval: time.Minute,
			},
			Retention: time.Hour,
		}
		_, err := NewSnapshotter(cfg)
		assert.Error(t, err)
	})

	t.Run("requires NATS URLs", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				ClusterID:       "cluster-1",
				NodeID:          "node-1",
				DBPath:          "/tmp/db.sqlite",
				SnapshotInterval: time.Minute,
			},
			Retention: time.Hour,
		}
		_, err := NewSnapshotter(cfg)
		assert.Error(t, err)
	})

	t.Run("requires DB path", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				ClusterID:       "cluster-1",
				NodeID:          "node-1",
				NATSURLs:        []string{"nats://localhost:4222"},
				SnapshotInterval: time.Minute,
			},
			Retention: time.Hour,
		}
		_, err := NewSnapshotter(cfg)
		assert.Error(t, err)
	})

	t.Run("requires positive snapshot interval", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				ClusterID: "cluster-1",
				NodeID:    "node-1",
				NATSURLs:  []string{"nats://localhost:4222"},
				DBPath:    "/tmp/db.sqlite",
			},
			Retention: time.Hour,
		}
		_, err := NewSnapshotter(cfg)
		assert.Error(t, err)
	})

	t.Run("requires positive retention", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				ClusterID:       "cluster-1",
				NodeID:          "node-1",
				NATSURLs:        []string{"nats://localhost:4222"},
				DBPath:          "/tmp/db.sqlite",
				SnapshotInterval: time.Minute,
			},
		}
		_, err := NewSnapshotter(cfg)
		assert.Error(t, err)
	})

	t.Run("valid config is accepted", func(t *testing.T) {
		cfg := SnapshotConfig{
			Config: Config{
				ClusterID:       "cluster-1",
				NodeID:          "node-1",
				NATSURLs:        []string{"nats://localhost:4222"},
				DBPath:          "/tmp/db.sqlite",
				SnapshotInterval: time.Minute,
			},
			Retention: time.Hour,
		}
		snap, err := NewSnapshotter(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, snap)
		assert.Equal(t, "convex-cluster-1-wal", snap.BucketName())
	})
}

func TestSnapshotterStartStopIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	// Connect and create the Object Store bucket expected by the replica client.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	bucketName := "convex-test-cluster-snapshot-idempotent-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a temporary SQLite database.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "snapshot-idempotent.db")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT);`)
	require.NoError(t, err)

	cfg := SnapshotConfig{
		Config: Config{
			ClusterID:       "test-cluster-snapshot-idempotent",
			NodeID:          "node-1",
			NATSURLs:        []string{natsContainer.URL},
			DBPath:          dbPath,
			SnapshotInterval: time.Minute,
		},
		Retention: time.Hour,
	}

	snap, err := NewSnapshotter(cfg)
	require.NoError(t, err)

	// Start/Stop should be idempotent.
	require.NoError(t, snap.Start(ctx))
	require.NoError(t, snap.Start(ctx))

	assert.True(t, snap.Running())

	require.NoError(t, snap.Stop(context.Background()))
	require.NoError(t, snap.Stop(context.Background()))

	assert.False(t, snap.Running())
}

func TestSnapshotterCreatesSnapshotsAndEnforcesRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	// Connect to NATS and create the Object Store bucket used by the snapshotter.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	clusterID := "test-cluster-snapshot"
	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a temporary SQLite database representing the Convex backend data path.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "snapshot.db")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT);`)
	require.NoError(t, err)

	// Insert a few rows so the snapshot has content.
	for i := 0; i < 3; i++ {
		_, err := db.ExecContext(ctx, `INSERT INTO events (message) VALUES (?)`, fmt.Sprintf("msg-%d", i))
		require.NoError(t, err)
	}

	cfg := SnapshotConfig{
		Config: Config{
			ClusterID:       clusterID,
			NodeID:          "node-snapshot",
			NATSURLs:        []string{natsContainer.URL},
			DBPath:          dbPath,
			SnapshotInterval: time.Hour, // large interval; tests drive snapshotOnce manually
		},
		Retention: 500 * time.Millisecond,
	}

	snap, err := NewSnapshotter(cfg)
	require.NoError(t, err)

	require.NoError(t, snap.Start(ctx))
	defer snap.Stop(context.Background())

	// Allow litestream to fully initialize (read page size from SQLite header, start sync monitors).
	time.Sleep(2 * time.Second)

	objectStore, err := js.ObjectStore(ctx, bucketName)
	require.NoError(t, err)

	// Helper to count snapshot-level objects only.
	countSnapshots := func() int {
		objects, err := objectStore.List(ctx)
		require.NoError(t, err)

		prefix := fmt.Sprintf("ltx/%d/", litestream.SnapshotLevel)
		count := 0
		for _, obj := range objects {
			if len(obj.Name) >= len(prefix) && obj.Name[:len(prefix)] == prefix {
				count++
			}
		}
		return count
	}

	// First snapshot
	require.NoError(t, snap.snapshotOnce(ctx))

	// Should have at least one snapshot object.
	firstCount := countSnapshots()
	if firstCount == 0 {
		t.Fatalf("expected at least one snapshot object in bucket %q, found none", bucketName)
	}

	// Wait long enough so that the first snapshot becomes older than the retention
	// window, then take a second snapshot which should trigger retention cleanup.
	time.Sleep(cfg.Retention + 200*time.Millisecond)

	// Additional writes so that the second snapshot represents a newer state.
	for i := 3; i < 6; i++ {
		_, err := db.ExecContext(ctx, `INSERT INTO events (message) VALUES (?)`, fmt.Sprintf("msg-%d", i))
		require.NoError(t, err)
	}

	require.NoError(t, snap.snapshotOnce(ctx))

	// After retention enforcement, there should only be a single snapshot-level
	// object remaining in the bucket.
	finalCount := countSnapshots()
	assert.Equal(t, 1, finalCount, "expected exactly one retained snapshot after retention enforcement")
}
