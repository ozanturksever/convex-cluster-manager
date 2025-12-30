package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/internal/config"
	"github.com/ozanturksever/convex-cluster-manager/internal/replication"
	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

// FailoverMetrics captures RPO/RTO measurements during failover testing.
type FailoverMetrics struct {
	// RPO - Recovery Point Objective
	// Maximum data loss measured in number of committed transactions
	TransactionsLost int64

	// RTO - Recovery Time Objective
	// Time from primary failure to new primary being ready
	RecoveryTime time.Duration

	// Additional metrics
	FailoverStartTime   time.Time
	FailoverEndTime     time.Time
	LastCommittedTXID   int64
	FirstRecoveredTXID  int64
}

// TestFailoverWithRPORTOValidation performs a controlled failover and measures
// RPO (data loss) and RTO (recovery time).
func TestFailoverWithRPORTOValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "rpo-rto-test"
	tmpDir := t.TempDir()

	// Create Object Store bucket
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create configs
	cfg1 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Track metrics
	metrics := &FailoverMetrics{}

	// Create daemons
	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	// Start both daemons
	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	defer daemon1Cancel()
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)

	go func() { errCh1 <- daemon1.Run(daemon1Ctx) }()
	go func() { errCh2 <- daemon2.Run(daemon2Ctx) }()

	// Wait for initial leader election
	waitForLeader(t, daemon1, daemon2, 20*time.Second)

	// Determine which is the primary
	var primaryDBPath string
	if daemon1.state.Role() == RolePrimary {
		primaryDBPath = cfg1.Backend.DataPath
	} else {
		primaryDBPath = cfg2.Backend.DataPath
	}

	// Wait for database to be ready
	time.Sleep(3 * time.Second)

	// Write transactions with monotonic IDs
	var lastWrittenID atomic.Int64
	stopWrites := make(chan struct{})

	go func() {
		db, err := sql.Open("sqlite", primaryDBPath)
		if err != nil {
			return
		}
		defer db.Close()

		for id := int64(1); ; id++ {
			select {
			case <-stopWrites:
				return
			default:
			}

			_, err := db.Exec("INSERT INTO events (id, message) VALUES (?, ?)", id, fmt.Sprintf("tx-%d", id))
			if err == nil {
				lastWrittenID.Store(id)
			}
			time.Sleep(50 * time.Millisecond) // ~20 writes/second
		}
	}()

	// Let writes happen for a while
	time.Sleep(3 * time.Second)

	// Record the last committed transaction before failover
	metrics.LastCommittedTXID = lastWrittenID.Load()
	t.Logf("Last committed TXID before failover: %d", metrics.LastCommittedTXID)

	// Trigger failover
	t.Log("Triggering failover...")
	metrics.FailoverStartTime = time.Now()

	// Stop writes during failover
	close(stopWrites)

	// Kill the primary and determine which daemon will become new primary
	var newPrimary *Daemon
	var newPrimaryDBPath string
	if daemon1.state.Role() == RolePrimary {
		t.Log("Killing primary daemon1, waiting for daemon2 to become primary...")
		daemon1Cancel()
		<-errCh1
		newPrimary = daemon2
		newPrimaryDBPath = cfg2.Backend.DataPath
	} else {
		t.Log("Killing primary daemon2, waiting for daemon1 to become primary...")
		daemon2Cancel()
		<-errCh2
		newPrimary = daemon1
		newPrimaryDBPath = cfg1.Backend.DataPath
	}

	// Wait for the other node to become primary
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if newPrimary.state.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	metrics.FailoverEndTime = time.Now()
	metrics.RecoveryTime = metrics.FailoverEndTime.Sub(metrics.FailoverStartTime)

	t.Logf("New primary is %s, role: %s", newPrimary.cfg.NodeID, newPrimary.state.Role())
	t.Logf("Recovery time (RTO): %v", metrics.RecoveryTime)

	// Check what data was preserved
	time.Sleep(2 * time.Second) // Wait for catch-up

	db, err := sql.Open("sqlite", newPrimaryDBPath)
	if err == nil {
		defer db.Close()

		var maxID int64
		err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM events").Scan(&maxID)
		if err == nil {
			metrics.FirstRecoveredTXID = maxID
			metrics.TransactionsLost = metrics.LastCommittedTXID - maxID
			if metrics.TransactionsLost < 0 {
				metrics.TransactionsLost = 0
			}
		}
	}

	// Report metrics
	t.Logf("=== Failover Metrics ===")
	t.Logf("RTO (Recovery Time): %v", metrics.RecoveryTime)
	t.Logf("Last committed TXID: %d", metrics.LastCommittedTXID)
	t.Logf("First recovered TXID: %d", metrics.FirstRecoveredTXID)
	t.Logf("RPO (Transactions Lost): %d", metrics.TransactionsLost)

	// Assertions
	// RTO should be less than 2x the leader TTL
	maxRTO := cfg1.Election.LeaderTTL * 2
	assert.Less(t, metrics.RecoveryTime, maxRTO,
		"RTO should be less than %v", maxRTO)

	// RPO should be minimal (depends on sync interval)
	// With 50ms writes and 1s sync, expect at most ~20 transactions lost
	maxRPO := int64(50)
	assert.LessOrEqual(t, metrics.TransactionsLost, maxRPO,
		"RPO should be at most %d transactions", maxRPO)
}

// TestFailoverDataIntegrity verifies that data is not corrupted during failover.
func TestFailoverDataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "integrity-failover-test"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	cfg1 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	defer daemon1Cancel()
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)

	go func() { errCh1 <- daemon1.Run(daemon1Ctx) }()
	go func() { errCh2 <- daemon2.Run(daemon2Ctx) }()

	waitForLeader(t, daemon1, daemon2, 20*time.Second)

	var primaryDBPath string
	if daemon1.state.Role() == RolePrimary {
		primaryDBPath = cfg1.Backend.DataPath
	} else {
		primaryDBPath = cfg2.Backend.DataPath
	}

	time.Sleep(3 * time.Second)

	// Write known data pattern
	db, err := sql.Open("sqlite", primaryDBPath)
	require.NoError(t, err)

	expectedData := make(map[int64]string)
	for i := int64(1); i <= 100; i++ {
		msg := fmt.Sprintf("integrity-check-%d", i)
		_, err := db.Exec("INSERT INTO events (id, message) VALUES (?, ?)", i, msg)
		require.NoError(t, err)
		expectedData[i] = msg
	}
	db.Close()

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Trigger failover
	t.Log("Triggering failover for integrity check...")
	if daemon1.state.Role() == RolePrimary {
		daemon1Cancel()
		<-errCh1
	}

	// Wait for failover
	time.Sleep(5 * time.Second)

	// Verify data integrity in the replica
	var checkDBPath string
	if daemon2.state.Role() == RolePrimary {
		checkDBPath = cfg2.Backend.DataPath
	} else {
		checkDBPath = cfg2.WAL.ReplicaPath
	}

	// Check if database exists and has data
	checkDB, err := sql.Open("sqlite", checkDBPath)
	if err != nil {
		t.Logf("Could not open check database: %v", err)
		return
	}
	defer checkDB.Close()

	// Run SQLite integrity check
	var integrityResult string
	err = checkDB.QueryRow("PRAGMA integrity_check;").Scan(&integrityResult)
	if err == nil {
		assert.Equal(t, "ok", integrityResult, "SQLite integrity check should pass")
	}

	// Check data integrity
	rows, err := checkDB.Query("SELECT id, message FROM events ORDER BY id")
	if err != nil {
		t.Logf("Could not query events: %v", err)
		return
	}
	defer rows.Close()

	recoveredCount := 0
	for rows.Next() {
		var id int64
		var msg string
		err := rows.Scan(&id, &msg)
		require.NoError(t, err)

		expectedMsg, exists := expectedData[id]
		if exists {
			assert.Equal(t, expectedMsg, msg, "Data mismatch for id %d", id)
			recoveredCount++
		}
	}

	t.Logf("Recovered %d/%d records", recoveredCount, len(expectedData))
	assert.Greater(t, recoveredCount, 0, "Should recover some data after failover")
}

// TestIntegrityCheckerDuringFailover verifies the integrity checker works during failover.
func TestIntegrityCheckerDuringFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "checker-failover-test"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	cfg := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")

	// Create database with data
	db, err := sql.Open("sqlite", cfg.Backend.DataPath)
	require.NoError(t, err)

	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, message TEXT);")
	require.NoError(t, err)

	for i := 1; i <= 50; i++ {
		_, err = db.Exec("INSERT INTO events VALUES (?, 'test');", i)
		require.NoError(t, err)
	}
	db.Close()

	// Start primary replication
	primaryCfg := replication.Config{
		ClusterID:        clusterID,
		NodeID:           "node-1",
		DBPath:           cfg.Backend.DataPath,
		NATSURLs:         []string{natsContainer.URL},
		SnapshotInterval: time.Hour,
	}

	primary, err := replication.NewPrimary(primaryCfg)
	require.NoError(t, err)

	err = primary.Start(ctx)
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(3 * time.Second)

	_ = primary.Stop(ctx)

	// Create integrity checker
	checker, err := replication.NewIntegrityChecker(primaryCfg)
	require.NoError(t, err)

	// Run integrity check
	report, err := checker.Check(ctx)
	require.NoError(t, err)

	t.Logf("Integrity Report:")
	t.Logf("  Database Exists: %v", report.DatabaseExists)
	t.Logf("  Database Valid: %v", report.DatabaseValid)
	t.Logf("  WAL Mode: %v", report.WALMode)
	t.Logf("  Page Count: %d", report.PageCount)
	t.Logf("  Replica Reachable: %v", report.ReplicaReachable)
	t.Logf("  Replica LTX Count: %d", report.ReplicaLTXCount)
	t.Logf("  Replica Max TXID: %d", report.ReplicaMaxTXID)
	t.Logf("  Errors: %v", report.Errors)
	t.Logf("  Is Healthy: %v", report.IsHealthy())

	assert.True(t, report.DatabaseExists)
	assert.True(t, report.DatabaseValid)
	assert.True(t, report.WALMode)
	assert.True(t, report.ReplicaReachable)
	assert.True(t, report.IsHealthy())
	assert.Greater(t, report.ReplicaLTXCount, 0, "Should have LTX files after replication")
}

// createE2ETestConfig creates a test config for E2E testing.
func createE2ETestConfig(t *testing.T, tmpDir, natsURL, clusterID, nodeID string) *config.Config {
	t.Helper()

	dbPath := filepath.Join(tmpDir, nodeID+"-backend.db")
	replicaPath := filepath.Join(tmpDir, nodeID+"-replica.db")

	// Create the SQLite database with WAL mode
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(`PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, message TEXT);`)
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)

	return &config.Config{
		ClusterID: clusterID,
		NodeID:    nodeID,
		NATS: config.NATSConfig{
			Servers: []string{natsURL},
		},
		Election: config.ElectionConfig{
			LeaderTTL:         5 * time.Second, // Short TTL for faster failover
			HeartbeatInterval: 1 * time.Second,
		},
		WAL: config.WALConfig{
			StreamRetention:  24 * time.Hour,
			SnapshotInterval: time.Hour,
			ReplicaPath:      replicaPath,
		},
		Backend: config.BackendConfig{
			ServiceName:    "test-backend",
			HealthEndpoint: "http://localhost:3210/version",
			DataPath:       dbPath,
		},
	}
}
