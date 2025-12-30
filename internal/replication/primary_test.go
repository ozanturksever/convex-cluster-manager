package replication

import (
	"context"
	"database/sql"
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

func TestPrimaryConfigValidation(t *testing.T) {
	t.Run("requires clusterID", func(t *testing.T) {
		cfg := Config{
			NodeID:   "node-1",
			NATSURLs: []string{"nats://localhost:4222"},
			DBPath:   "/tmp/db.sqlite",
		}
		_, err := NewPrimary(cfg)
		assert.Error(t, err)
	})

	t.Run("requires nodeID", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NATSURLs:  []string{"nats://localhost:4222"},
			DBPath:    "/tmp/db.sqlite",
		}
		_, err := NewPrimary(cfg)
		assert.Error(t, err)
	})

	t.Run("requires NATS URLs", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			DBPath:    "/tmp/db.sqlite",
		}
		_, err := NewPrimary(cfg)
		assert.Error(t, err)
	})

	t.Run("requires DB path", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		_, err := NewPrimary(cfg)
		assert.Error(t, err)
	})

	t.Run("valid config is accepted", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATSURLs:  []string{"nats://localhost:4222"},
			DBPath:    "/tmp/db.sqlite",
		}
		primary, err := NewPrimary(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, primary)
		assert.Equal(t, "convex-cluster-1-wal", primary.BucketName())
	})
}

func TestPrimaryStartStopIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	// Connect and create the Object Store bucket expected by the replica client.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	bucketName := "convex-test-cluster-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a temporary SQLite database.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "primary-idempotent.db")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT);`)
	require.NoError(t, err)

	cfg := Config{
		ClusterID: "test-cluster",
		NodeID:    "node-1",
		NATSURLs:  []string{natsContainer.URL},
		DBPath:    dbPath,
	}

	primary, err := NewPrimary(cfg)
	require.NoError(t, err)

	// Start/Stop should be idempotent.
	require.NoError(t, primary.Start(ctx))
	require.NoError(t, primary.Start(ctx))

	require.True(t, primary.Running())

	require.NoError(t, primary.Stop(context.Background()))
	require.NoError(t, primary.Stop(context.Background()))

	assert.False(t, primary.Running())
}

func TestPrimaryReplicatesWALToNATS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	// Connect to NATS and create the Object Store bucket used by the primary.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	clusterID := "test-cluster"
	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a temporary SQLite database representing the Convex backend data path.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "primary-replication.db")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT);`)
	require.NoError(t, err)

	cfg := Config{
		ClusterID: clusterID,
		NodeID:    "node-1",
		NATSURLs:  []string{natsContainer.URL},
		DBPath:    dbPath,
	}

	primary, err := NewPrimary(cfg)
	require.NoError(t, err)

	require.NoError(t, primary.Start(ctx))
	defer func() { _ = primary.Stop(context.Background()) }()

	// Allow litestream to fully initialize its internal state (monitors, shadow WAL, etc.)
	time.Sleep(2 * time.Second)

	// Perform some writes to generate WAL activity.
	for i := 0; i < 5; i++ {
		_, err := db.ExecContext(ctx, `INSERT INTO events (message) VALUES (?)`, fmt.Sprintf("msg-%d", i))
		require.NoError(t, err)
		// Small pause to give litestream time to notice changes.
		time.Sleep(100 * time.Millisecond)
	}

	// Force a WAL checkpoint to ensure all changes are visible to litestream.
	_, err = db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE);`)
	require.NoError(t, err)

	// Wait for LTX files to appear in the Object Store bucket.
	objectStore, err := js.ObjectStore(ctx, bucketName)
	require.NoError(t, err)

	deadline := time.Now().Add(15 * time.Second)
	var objects []*jetstream.ObjectInfo

	for {
		objects, err = objectStore.List(ctx)
		require.NoError(t, err)

		if len(objects) > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// We expect at least one LTX object to have been written.
	if len(objects) == 0 {
		// Provide more context on failure.
		t.Fatalf("expected at least one LTX object in bucket %q, found none", bucketName)
	}

	// Sanity-check that object names look like LTX paths (prefix ltx/).
	foundLTXPrefix := false
	for _, obj := range objects {
		if len(obj.Name) >= 4 && obj.Name[:4] == "ltx/" {
			foundLTXPrefix = true
			break
		}
	}
	assert.True(t, foundLTXPrefix, "expected at least one object with 'ltx/' prefix in name")
}
