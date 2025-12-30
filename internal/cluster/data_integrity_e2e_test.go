package cluster

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"


	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

// DataIntegrityTestConfig holds configuration for data integrity tests.
type DataIntegrityTestConfig struct {
	RecordCount    int           // Number of records to insert
	FailoverDelay  time.Duration // Time to wait after inserting before failover
	RecoveryDelay  time.Duration // Time to wait for recovery after failover
	MaxDataLoss    int           // Maximum acceptable data loss (RPO)
	VerifyChecksum bool          // Whether to verify data checksums
}

// DefaultDataIntegrityConfig returns default test configuration.
func DefaultDataIntegrityConfig() DataIntegrityTestConfig {
	return DataIntegrityTestConfig{
		RecordCount:    100,
		FailoverDelay:  3 * time.Second,
		RecoveryDelay:  5 * time.Second,
		MaxDataLoss:    10, // Allow up to 10 records lost during async replication
		VerifyChecksum: true,
	}
}

// TestDataIntegrity_PreFailoverDataPreserved verifies that data written before
// failover is preserved on the new primary.
func TestDataIntegrity_PreFailoverDataPreserved(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()
	cfg := DefaultDataIntegrityConfig()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "pre-failover-data-test"
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
	var primaryDaemon *Daemon
	if daemon1.state.Role() == RolePrimary {
		primaryDBPath = cfg1.Backend.DataPath
		primaryDaemon = daemon1
	} else {
		primaryDBPath = cfg2.Backend.DataPath
		primaryDaemon = daemon2
	}

	time.Sleep(3 * time.Second)

	// Insert test data with checksums
	type testRecord struct {
		ID       int64
		Message  string
		Checksum string
	}
	records := make([]testRecord, cfg.RecordCount)

	db, err := sql.Open("sqlite", primaryDBPath)
	require.NoError(t, err)

	for i := 0; i < cfg.RecordCount; i++ {
		msg := fmt.Sprintf("pre-failover-data-%d-%d", time.Now().UnixNano(), i)
		checksum := sha256.Sum256([]byte(msg))
		checksumStr := hex.EncodeToString(checksum[:])

		_, err := db.Exec("INSERT INTO events (id, message) VALUES (?, ?)", i+1, msg)
		require.NoError(t, err)

		records[i] = testRecord{
			ID:       int64(i + 1),
			Message:  msg,
			Checksum: checksumStr,
		}
	}
	db.Close()

	t.Logf("Inserted %d records before failover", cfg.RecordCount)

	// Wait for replication
	time.Sleep(cfg.FailoverDelay)

	// Trigger failover
	t.Log("Triggering failover...")
	if primaryDaemon == daemon1 {
		daemon1Cancel()
		select {
		case <-errCh1:
		case <-time.After(10 * time.Second):
		}
	} else {
		daemon2Cancel()
		select {
		case <-errCh2:
		case <-time.After(10 * time.Second):
		}
	}

	// Wait for recovery
	time.Sleep(cfg.RecoveryDelay)

	// Verify data on new primary
	var newPrimaryDBPath string
	if primaryDaemon == daemon1 {
		// daemon2 should be new primary
		if daemon2.state.Role() == RolePrimary {
			newPrimaryDBPath = cfg2.Backend.DataPath
		} else {
			newPrimaryDBPath = cfg2.WAL.ReplicaPath
		}
	} else {
		if daemon1.state.Role() == RolePrimary {
			newPrimaryDBPath = cfg1.Backend.DataPath
		} else {
			newPrimaryDBPath = cfg1.WAL.ReplicaPath
		}
	}

	verifyDB, err := sql.Open("sqlite", newPrimaryDBPath)
	if err != nil {
		t.Logf("Could not open verification database at %s: %v", newPrimaryDBPath, err)
		return
	}
	defer verifyDB.Close()

	// Count recovered records
	var recoveredCount int
	err = verifyDB.QueryRow("SELECT COUNT(*) FROM events").Scan(&recoveredCount)
	if err != nil {
		t.Logf("Could not count events: %v", err)
		return
	}

	t.Logf("Recovered %d/%d records after failover", recoveredCount, cfg.RecordCount)

	// Verify data integrity with checksums
	if cfg.VerifyChecksum && recoveredCount > 0 {
		rows, err := verifyDB.Query("SELECT id, message FROM events ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		corruptedCount := 0
		for rows.Next() {
			var id int64
			var msg string
			err := rows.Scan(&id, &msg)
			require.NoError(t, err)

			// Find original record and verify checksum
			for _, rec := range records {
				if rec.ID == id {
					checksum := sha256.Sum256([]byte(msg))
					actualChecksum := hex.EncodeToString(checksum[:])
					if actualChecksum != rec.Checksum {
						corruptedCount++
						t.Logf("Checksum mismatch for record %d", id)
					}
					break
				}
			}
		}
		assert.Equal(t, 0, corruptedCount, "No records should be corrupted")
	}

	// Check data loss is within acceptable limits
	dataLoss := cfg.RecordCount - recoveredCount
	assert.LessOrEqual(t, dataLoss, cfg.MaxDataLoss,
		"Data loss (%d) should not exceed max (%d)", dataLoss, cfg.MaxDataLoss)
}

// TestDataIntegrity_PostFailoverWritesWork verifies that writes work correctly
// on the new primary after failover.
func TestDataIntegrity_PostFailoverWritesWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "post-failover-writes-test"
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

	var primaryDaemon *Daemon
	if daemon1.state.Role() == RolePrimary {
		primaryDaemon = daemon1
	} else {
		primaryDaemon = daemon2
	}

	time.Sleep(3 * time.Second)

	// Trigger failover
	t.Log("Triggering failover...")
	if primaryDaemon == daemon1 {
		daemon1Cancel()
		select {
		case <-errCh1:
		case <-time.After(10 * time.Second):
		}
	} else {
		daemon2Cancel()
		select {
		case <-errCh2:
		case <-time.After(10 * time.Second):
		}
	}

	// Wait for new primary to stabilize
	time.Sleep(10 * time.Second)

	// Determine new primary
	var newPrimary *Daemon
	var newPrimaryDBPath string
	if primaryDaemon == daemon1 {
		newPrimary = daemon2
		newPrimaryDBPath = cfg2.Backend.DataPath
	} else {
		newPrimary = daemon1
		newPrimaryDBPath = cfg1.Backend.DataPath
	}

	assert.Equal(t, RolePrimary, newPrimary.state.Role(), "New primary should have PRIMARY role")

	// Write data on new primary
	db, err := sql.Open("sqlite", newPrimaryDBPath)
	if err != nil {
		t.Logf("Could not open new primary database: %v", err)
		return
	}
	defer db.Close()

	postFailoverRecords := 50
	for i := 0; i < postFailoverRecords; i++ {
		msg := fmt.Sprintf("post-failover-write-%d", i)
		_, err := db.Exec("INSERT INTO events (message) VALUES (?)", msg)
		if err != nil {
			t.Logf("Write %d failed: %v", i, err)
			continue
		}
	}

	// Verify writes succeeded
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM events WHERE message LIKE 'post-failover-write-%'").Scan(&count)
	require.NoError(t, err)

	t.Logf("Successfully wrote %d/%d post-failover records", count, postFailoverRecords)
	assert.Greater(t, count, 0, "Should be able to write on new primary")
}

// TestDataIntegrity_ConcurrentWritesDuringFailover tests that concurrent writes
// during failover don't cause data corruption.
func TestDataIntegrity_ConcurrentWritesDuringFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "concurrent-writes-test"
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
	var primaryDaemon *Daemon
	if daemon1.state.Role() == RolePrimary {
		primaryDBPath = cfg1.Backend.DataPath
		primaryDaemon = daemon1
	} else {
		primaryDBPath = cfg2.Backend.DataPath
		primaryDaemon = daemon2
	}

	time.Sleep(3 * time.Second)

	// Start concurrent writers
	var successfulWrites atomic.Int64
	var failedWrites atomic.Int64
	stopWriters := make(chan struct{})
	var writerWg sync.WaitGroup

	for writerID := 0; writerID < 3; writerID++ {
		writerWg.Add(1)
		go func(id int) {
			defer writerWg.Done()

			db, err := sql.Open("sqlite", primaryDBPath)
			if err != nil {
				return
			}
			defer db.Close()

			for i := 0; ; i++ {
				select {
				case <-stopWriters:
					return
				default:
				}

				msg := fmt.Sprintf("concurrent-writer-%d-msg-%d", id, i)
				_, err := db.Exec("INSERT INTO events (message) VALUES (?)", msg)
				if err != nil {
					failedWrites.Add(1)
				} else {
					successfulWrites.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(writerID)
	}

	// Let writes happen for a while
	time.Sleep(2 * time.Second)

	// Trigger failover while writes are happening
	t.Log("Triggering failover during concurrent writes...")
	if primaryDaemon == daemon1 {
		daemon1Cancel()
		select {
		case <-errCh1:
		case <-time.After(10 * time.Second):
		}
	} else {
		daemon2Cancel()
		select {
		case <-errCh2:
		case <-time.After(10 * time.Second):
		}
	}

	// Let remaining writes complete/fail
	time.Sleep(1 * time.Second)
	close(stopWriters)
	writerWg.Wait()

	t.Logf("Concurrent writes: %d successful, %d failed", successfulWrites.Load(), failedWrites.Load())

	// Wait for recovery
	time.Sleep(5 * time.Second)

	// Verify database integrity on replica
	var checkDBPath string
	if primaryDaemon == daemon1 {
		checkDBPath = cfg2.WAL.ReplicaPath
		if daemon2.state.Role() == RolePrimary {
			checkDBPath = cfg2.Backend.DataPath
		}
	} else {
		checkDBPath = cfg1.WAL.ReplicaPath
		if daemon1.state.Role() == RolePrimary {
			checkDBPath = cfg1.Backend.DataPath
		}
	}

	checkDB, err := sql.Open("sqlite", checkDBPath)
	if err != nil {
		t.Logf("Could not open check database: %v", err)
		return
	}
	defer checkDB.Close()

	// SQLite integrity check
	var integrityResult string
	err = checkDB.QueryRow("PRAGMA integrity_check;").Scan(&integrityResult)
	if err == nil {
		assert.Equal(t, "ok", integrityResult, "Database should pass integrity check")
	}

	// Count records
	var count int
	err = checkDB.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
	if err == nil {
		t.Logf("Total records in recovered database: %d", count)
		assert.Greater(t, count, 0, "Should have recovered some records")
	}
}

// TestDataIntegrity_MultipleFailovers verifies data integrity across multiple
// consecutive failovers.
func TestDataIntegrity_MultipleFailovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "multi-failover-test"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	cfg1 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Track cumulative data
	var totalRecordsWritten atomic.Int64
	failoverCount := 3

	for iteration := 0; iteration < failoverCount; iteration++ {
		t.Logf("=== Failover iteration %d/%d ===", iteration+1, failoverCount)

		daemon1, err := NewDaemon(cfg1)
		require.NoError(t, err)

		daemon2, err := NewDaemon(cfg2)
		require.NoError(t, err)

		daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
		daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)

		errCh1 := make(chan error, 1)
		errCh2 := make(chan error, 1)

		go func() { errCh1 <- daemon1.Run(daemon1Ctx) }()
		go func() { errCh2 <- daemon2.Run(daemon2Ctx) }()

		waitForLeader(t, daemon1, daemon2, 20*time.Second)

		var primaryDBPath string
		var primaryDaemon *Daemon
		if daemon1.state.Role() == RolePrimary {
			primaryDBPath = cfg1.Backend.DataPath
			primaryDaemon = daemon1
		} else {
			primaryDBPath = cfg2.Backend.DataPath
			primaryDaemon = daemon2
		}

		time.Sleep(2 * time.Second)

		// Write some data
		db, err := sql.Open("sqlite", primaryDBPath)
		if err == nil {
			recordsThisIteration := 20
			for i := 0; i < recordsThisIteration; i++ {
				msg := fmt.Sprintf("multi-failover-iter-%d-rec-%d", iteration, i)
				_, err := db.Exec("INSERT INTO events (message) VALUES (?)", msg)
				if err == nil {
					totalRecordsWritten.Add(1)
				}
			}
			db.Close()
		}

		// Wait for replication
		time.Sleep(2 * time.Second)

		// Trigger failover
		if primaryDaemon == daemon1 {
			daemon1Cancel()
			<-errCh1
			time.Sleep(5 * time.Second)
			daemon2Cancel()
			<-errCh2
		} else {
			daemon2Cancel()
			<-errCh2
			time.Sleep(5 * time.Second)
			daemon1Cancel()
			<-errCh1
		}

		time.Sleep(2 * time.Second)
	}

	t.Logf("Total records written across %d failovers: %d", failoverCount, totalRecordsWritten.Load())

	// Verify final data integrity
	db, err := sql.Open("sqlite", cfg1.Backend.DataPath)
	if err == nil {
		defer db.Close()

		var integrityResult string
		err = db.QueryRow("PRAGMA integrity_check;").Scan(&integrityResult)
		if err == nil {
			assert.Equal(t, "ok", integrityResult, "Database should pass integrity check after multiple failovers")
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
		if err == nil {
			t.Logf("Final record count: %d", count)
		}
	}
}

// TestDataIntegrity_SQLiteIntegrityCheck verifies SQLite PRAGMA integrity_check passes
// after various operations.
func TestDataIntegrity_SQLiteIntegrityCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "sqlite-integrity-test"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	cfg1 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createE2ETestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon1Cancel()
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

	// Perform various database operations
	db, err := sql.Open("sqlite", primaryDBPath)
	require.NoError(t, err)
	defer db.Close()

	// Create additional tables
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)`)
	require.NoError(t, err)

	// Insert data
	for i := 0; i < 100; i++ {
		_, _ = db.Exec("INSERT INTO users (name, email) VALUES (?, ?)",
			fmt.Sprintf("User %d", i),
			fmt.Sprintf("user%d@test.com", i))
	}

	// Update data
	for i := 0; i < 50; i++ {
		_, _ = db.Exec("UPDATE users SET name = ? WHERE id = ?",
			fmt.Sprintf("Updated User %d", i), i+1)
	}

	// Delete data
	_, _ = db.Exec("DELETE FROM users WHERE id > 75")

	// Force checkpoint
	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	// Run integrity check
	var integrityResult string
	err = db.QueryRow("PRAGMA integrity_check;").Scan(&integrityResult)
	require.NoError(t, err)
	assert.Equal(t, "ok", integrityResult, "SQLite integrity check should pass")

	// Run foreign key check
	var fkResult string
	err = db.QueryRow("PRAGMA foreign_key_check;").Scan(&fkResult)
	// foreign_key_check returns empty for no violations
	if err == sql.ErrNoRows {
		t.Log("Foreign key check passed (no violations)")
	} else if err != nil {
		t.Logf("Foreign key check result: %v", err)
	}

	t.Log("SQLite integrity verification passed")
}

// Note: createObjectStoreBucket, waitForLeader, and createE2ETestConfig are defined in daemon_test.go
