package replication

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

func TestIntegrityChecker_CheckLocalDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a valid SQLite database
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test VALUES (1, 'hello');")
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// Create integrity checker
	cfg := Config{
		ClusterID: "test-cluster",
		NodeID:    "test-node",
		DBPath:    dbPath,
		NATSURLs:  []string{}, // No NATS for this test
	}
	checker, err := NewIntegrityChecker(cfg)
	require.NoError(t, err)

	// Run check
	ctx := context.Background()
	report, err := checker.Check(ctx)
	require.NoError(t, err)

	assert.True(t, report.DatabaseExists)
	assert.True(t, report.DatabaseValid)
	assert.True(t, report.WALMode)
	assert.Greater(t, report.PageCount, int64(0))
	assert.True(t, report.IsHealthy())
}

func TestIntegrityChecker_CheckNonExistentDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nonexistent.db")

	cfg := Config{
		ClusterID: "test-cluster",
		NodeID:    "test-node",
		DBPath:    dbPath,
		NATSURLs:  []string{},
	}
	checker, err := NewIntegrityChecker(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	report, err := checker.Check(ctx)
	require.NoError(t, err)

	assert.False(t, report.DatabaseExists)
	assert.True(t, report.IsHealthy()) // Non-existent is acceptable
}

func TestIntegrityChecker_CheckWithNATSReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "integrity-test"
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create Object Store bucket
	createTestObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create a valid SQLite database
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);")
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// Create integrity checker with NATS
	cfg := Config{
		ClusterID: clusterID,
		NodeID:    "test-node",
		DBPath:    dbPath,
		NATSURLs:  []string{natsContainer.URL},
	}
	checker, err := NewIntegrityChecker(cfg)
	require.NoError(t, err)

	// Run check
	report, err := checker.Check(ctx)
	require.NoError(t, err)

	assert.True(t, report.DatabaseExists)
	assert.True(t, report.DatabaseValid)
	assert.True(t, report.WALMode)
	assert.True(t, report.ReplicaReachable)
	// Initially no LTX files since we haven't replicated yet
	assert.Equal(t, 0, report.ReplicaLTXCount)
}

func TestIntegrityChecker_ValidateLTXAfterReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "ltx-validation-test"
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create Object Store bucket
	createTestObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create database with initial data
	sqldb, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	_, err = sqldb.Exec("PRAGMA journal_mode = WAL;")
	require.NoError(t, err)

	_, err = sqldb.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = sqldb.Exec("INSERT INTO test VALUES (?, 'data');", i)
		require.NoError(t, err)
	}
	err = sqldb.Close()
	require.NoError(t, err)

	// Start primary replication
	primaryCfg := Config{
		ClusterID:        clusterID,
		NodeID:           "test-node",
		DBPath:           dbPath,
		NATSURLs:         []string{natsContainer.URL},
		SnapshotInterval: time.Hour, // Long interval, we'll trigger manually
	}

	primary, err := NewPrimary(primaryCfg)
	require.NoError(t, err)

	err = primary.Start(ctx)
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Stop primary - ignore transaction cleanup errors from litestream
	// The error "sql: transaction has already been committed or rolled back" is a known
	// benign error that occurs during Litestream's Store.Close() cleanup.
	err = primary.Stop(ctx)
	if err != nil && !strings.Contains(err.Error(), "transaction has already been committed or rolled back") {
		require.NoError(t, err)
	}

	// Now check integrity
	checker, err := NewIntegrityChecker(primaryCfg)
	require.NoError(t, err)

	report, err := checker.Check(ctx)
	require.NoError(t, err)

	t.Logf("Integrity report: LTXCount=%d, MaxTXID=%d", report.ReplicaLTXCount, report.ReplicaMaxTXID)

	assert.True(t, report.DatabaseValid)
	assert.True(t, report.ReplicaReachable)
	assert.Greater(t, report.ReplicaLTXCount, 0, "Should have replicated LTX files")
	assert.Greater(t, report.ReplicaMaxTXID, uint64(0), "Should have non-zero max TXID")
}

func TestIntegrityReport_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		report   IntegrityReport
		expected bool
	}{
		{
			name:     "no database is healthy",
			report:   IntegrityReport{DatabaseExists: false},
			expected: true,
		},
		{
			name: "valid database is healthy",
			report: IntegrityReport{
				DatabaseExists: true,
				DatabaseValid:  true,
				WALMode:        true,
			},
			expected: true,
		},
		{
			name: "invalid database is unhealthy",
			report: IntegrityReport{
				DatabaseExists: true,
				DatabaseValid:  false,
				WALMode:        true,
			},
			expected: false,
		},
		{
			name: "non-WAL mode is unhealthy",
			report: IntegrityReport{
				DatabaseExists: true,
				DatabaseValid:  true,
				WALMode:        false,
			},
			expected: false,
		},
		{
			name: "errors make report unhealthy",
			report: IntegrityReport{
				DatabaseExists: true,
				DatabaseValid:  true,
				WALMode:        true,
				Errors:         []error{os.ErrNotExist},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.report.IsHealthy())
		})
	}
}

// Helper function to create Object Store bucket for tests
func createTestObjectStoreBucket(ctx context.Context, t *testing.T, natsURL, clusterID string) {
	t.Helper()

	nc, err := testutil.NATSConnect(natsURL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	bucketName := "convex-" + clusterID + "-wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   bucketName,
		Replicas: 1,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = js.DeleteObjectStore(cleanupCtx, bucketName)
	})
}
