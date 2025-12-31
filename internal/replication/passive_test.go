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

func TestPassiveConfigValidation(t *testing.T) {
	t.Run("requires clusterID", func(t *testing.T) {
		cfg := Config{
			NodeID:   "node-1",
			NATSURLs: []string{"nats://localhost:4222"},
			DBPath:   "/tmp/db.sqlite",
		}
		_, err := NewPassive(cfg)
		assert.Error(t, err)
	})

	t.Run("requires nodeID", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NATSURLs:  []string{"nats://localhost:4222"},
			DBPath:    "/tmp/db.sqlite",
		}
		_, err := NewPassive(cfg)
		assert.Error(t, err)
	})

	t.Run("requires NATS URLs", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			DBPath:    "/tmp/db.sqlite",
		}
		_, err := NewPassive(cfg)
		assert.Error(t, err)
	})

	t.Run("requires DB path", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		_, err := NewPassive(cfg)
		assert.Error(t, err)
	})

	t.Run("valid config is accepted", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATSURLs:  []string{"nats://localhost:4222"},
			DBPath:    "/tmp/db.sqlite",
		}
		passive, err := NewPassive(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, passive)
		assert.Equal(t, "convex-cluster-1-wal", passive.BucketName())
	})
}

func TestPassiveStartStopIdempotent(t *testing.T) {
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

	bucketName := "convex-test-cluster-passive-idempotent-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a temporary shadow SQLite database path (file will be created by restore).
	tmpDir := t.TempDir()
	shadowPath := filepath.Join(tmpDir, "shadow-idempotent.db")

	cfg := Config{
		ClusterID: "test-cluster-passive-idempotent",
		NodeID:    "node-passive",
		NATSURLs:  []string{natsContainer.URL},
		DBPath:    shadowPath,
	}

	passive, err := NewPassive(cfg)
	require.NoError(t, err)

	// Start/Stop should be idempotent.
	require.NoError(t, passive.Start(ctx))
	require.NoError(t, passive.Start(ctx))

	assert.True(t, passive.Running())

	require.NoError(t, passive.Stop(context.Background()))
	require.NoError(t, passive.Stop(context.Background()))

	assert.False(t, passive.Running())
}

func TestPassiveCatchUpEliminatesLag(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container with JetStream enabled.
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	// Connect to NATS and create the Object Store bucket used by both primary and passive.
	nc, err := nats.Connect(natsContainer.URL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	clusterID := "test-cluster-passive"
	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	require.NoError(t, err)

	// Create a temporary SQLite database representing the primary (active) backend.
	primaryDir := t.TempDir()
	primaryDBPath := filepath.Join(primaryDir, "primary.db")

	primaryDB, err := sql.Open("sqlite", primaryDBPath)
	require.NoError(t, err)
	defer primaryDB.Close()

	_, err = primaryDB.ExecContext(ctx, `PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = primaryDB.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`)
	require.NoError(t, err)
	_, err = primaryDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT);`)
	require.NoError(t, err)

	// Start primary replication so writes are streamed into the Object Store.
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

	// Allow litestream to fully initialize its internal state.
	time.Sleep(2 * time.Second)

	// Perform writes on the primary to generate WAL/LTX activity.
	for i := 0; i < 5; i++ {
		_, err := primaryDB.ExecContext(ctx, `INSERT INTO events (message) VALUES (?)`, fmt.Sprintf("msg-%d", i))
		require.NoError(t, err)
		// Small pause to give litestream time to notice and sync changes.
		time.Sleep(100 * time.Millisecond)
	}

	// Force a WAL checkpoint to ensure all changes are visible to litestream.
	_, err = primaryDB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE);`)
	require.NoError(t, err)

	// Wait for litestream to sync the changes to NATS.
	time.Sleep(2 * time.Second)

	// Create a shadow DB path for the passive node.
	shadowDir := t.TempDir()
	shadowDBPath := filepath.Join(shadowDir, "shadow.db")

	passiveCfg := Config{
		ClusterID: clusterID,
		NodeID:    "node-passive",
		NATSURLs:  []string{natsContainer.URL},
		DBPath:    shadowDBPath,
	}

	passive, err := NewPassive(passiveCfg)
	require.NoError(t, err)

	require.NoError(t, passive.Start(ctx))
	defer func() { _ = passive.Stop(context.Background()) }()

	// Perform a catch-up restore from NATS into the shadow DB.
	result, err := passive.CatchUp(ctx)
	require.NoError(t, err)
	assert.True(t, result.Restored, "CatchUp should have restored data")

	// Verify that the shadow DB now contains the same number of rows as the primary.
	var primaryCount int
	require.NoError(t, primaryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&primaryCount))

	shadowDB, err := sql.Open("sqlite", shadowDBPath)
	require.NoError(t, err)
	defer shadowDB.Close()

	var shadowCount int
	require.NoError(t, shadowDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&shadowCount))

	assert.Equal(t, primaryCount, shadowCount, "shadow DB should have same row count after catch-up")

	// After catch-up, the reported replication lag should be zero.
	lag, err := passive.ReplicationLag(ctx)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), lag)
}
