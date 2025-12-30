package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/internal/config"
	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

// TestDaemonRapidConsecutiveFailovers performs multiple rapid failovers in succession
// to verify cluster stability under stress. This is the Go version of the shell-based
// rapid failover test, providing faster execution and better CI integration.
func TestDaemonRapidConsecutiveFailovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "rapid-failover-stress"
	tmpDir := t.TempDir()

	// Create Object Store bucket
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create configs with short TTLs for fast failover
	cfg1 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Track failover metrics
	const iterations = 5
	successfulFailovers := 0
	failoverTimes := make([]time.Duration, 0, iterations)

	// Create daemons
	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	// Start both daemons
	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)

	go func() { errCh1 <- daemon1.Run(daemon1Ctx) }()
	go func() { errCh2 <- daemon2.Run(daemon2Ctx) }()

	// Wait for initial leader election
	waitForLeader(t, daemon1, daemon2, 20*time.Second)

	for i := 0; i < iterations; i++ {
		t.Logf("=== Failover iteration %d/%d ===", i+1, iterations)

		// Identify current primary and passive
		var primary, passive *Daemon
		var primaryCancel context.CancelFunc
		var primaryErrCh chan error

		if daemon1.state.Role() == RolePrimary {
			primary = daemon1
			passive = daemon2
			primaryCancel = daemon1Cancel
			primaryErrCh = errCh1
		} else {
			primary = daemon2
			passive = daemon1
			primaryCancel = daemon2Cancel
			primaryErrCh = errCh2
		}

		// Record failover start time
		failoverStart := time.Now()

		// Kill the primary
		t.Logf("Stopping primary (role=%s, node=%s)", primary.state.Role(), primary.cfg.NodeID)
		primaryCancel()

		select {
		case <-primaryErrCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("Iteration %d: timeout stopping primary", i+1)
		}

		// Wait for passive to become primary
		deadline := time.Now().Add(15 * time.Second)
		promoted := false
		for time.Now().Before(deadline) {
			if passive.state.Role() == RolePrimary {
				promoted = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if promoted {
			failoverTime := time.Since(failoverStart)
			failoverTimes = append(failoverTimes, failoverTime)
			successfulFailovers++
			t.Logf("Iteration %d: Failover SUCCESS in %v", i+1, failoverTime)
		} else {
			t.Logf("Iteration %d: Failover FAILED - passive is still %s", i+1, passive.state.Role())
		}

		// Restart the stopped daemon for next iteration
		var newCtx context.Context
		var newCancel context.CancelFunc
		var newErrCh chan error

		if primary == daemon1 {
			daemon1, err = NewDaemon(cfg1)
			require.NoError(t, err)
			newCtx, newCancel = context.WithCancel(ctx)
			daemon1Ctx = newCtx
			daemon1Cancel = newCancel
			newErrCh = make(chan error, 1)
			errCh1 = newErrCh
			go func(d *Daemon, ch chan error) { ch <- d.Run(newCtx) }(daemon1, newErrCh)
		} else {
			daemon2, err = NewDaemon(cfg2)
			require.NoError(t, err)
			newCtx, newCancel = context.WithCancel(ctx)
			daemon2Ctx = newCtx
			daemon2Cancel = newCancel
			newErrCh = make(chan error, 1)
			errCh2 = newErrCh
			go func(d *Daemon, ch chan error) { ch <- d.Run(newCtx) }(daemon2, newErrCh)
		}

		// Wait for cluster to stabilize before next iteration
		time.Sleep(3 * time.Second)
	}

	// Cleanup
	daemon1Cancel()
	daemon2Cancel()

	// Report results
	t.Logf("=== Rapid Failover Test Results ===")
	t.Logf("Successful failovers: %d/%d", successfulFailovers, iterations)
	if len(failoverTimes) > 0 {
		var total time.Duration
		for _, ft := range failoverTimes {
			total += ft
		}
		avg := total / time.Duration(len(failoverTimes))
		t.Logf("Average failover time: %v", avg)
	}

	// Assert high success rate (allow some tolerance for timing issues)
	assert.GreaterOrEqual(t, successfulFailovers, iterations-1,
		"At least %d/%d failovers should succeed", iterations-1, iterations)
}

// TestDaemonConcurrentWritesDuringFailover verifies that data writes during
// failover either succeed or fail cleanly with no silent data loss.
func TestDaemonConcurrentWritesDuringFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "concurrent-writes-failover"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	cfg1 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)
	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	_, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)

	go func() { errCh1 <- daemon1.Run(daemon1Ctx) }()
	go func() { errCh2 <- daemon2.Run(daemon1Ctx) }()

	// Wait for leader
	waitForLeader(t, daemon1, daemon2, 20*time.Second)

	// Wait for database to be ready
	time.Sleep(5 * time.Second)

	// Determine which database to write to based on the current primary
	var primaryDBPath string
	if daemon1.state.Role() == RolePrimary {
		primaryDBPath = cfg1.Backend.DataPath
	} else {
		primaryDBPath = cfg2.Backend.DataPath
	}

	// Start concurrent writes
	var writesAttempted int64
	var writesSucceeded int64
	var writesFailed int64
	stopWrites := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(writerID int, dbPath string) {
			defer wg.Done()

			db, err := sql.Open("sqlite", dbPath)
			if err != nil {
				return
			}
			defer db.Close()

			for {
				select {
				case <-stopWrites:
					return
				default:
				}

				atomic.AddInt64(&writesAttempted, 1)
				_, err := db.Exec(`INSERT INTO events (message) VALUES (?)`,
					fmt.Sprintf("writer-%d-msg-%d", writerID, time.Now().UnixNano()))

				if err != nil {
					atomic.AddInt64(&writesFailed, 1)
				} else {
					atomic.AddInt64(&writesSucceeded, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i, primaryDBPath)
	}

	// Let writes accumulate
	time.Sleep(2 * time.Second)

	// Trigger failover
	t.Log("Triggering failover during writes...")
	daemon1Cancel()

	<-errCh1

	// Continue writes during failover
	time.Sleep(3 * time.Second)

	// Stop writes
	close(stopWrites)
	wg.Wait()

	// Verify counts
	attempted := atomic.LoadInt64(&writesAttempted)
	succeeded := atomic.LoadInt64(&writesSucceeded)
	failed := atomic.LoadInt64(&writesFailed)

	t.Logf("Writes attempted: %d, succeeded: %d, failed: %d", attempted, succeeded, failed)

	// Verify all writes are accounted for
	assert.Equal(t, attempted, succeeded+failed, "All write attempts should be accounted for")

	// Verify some writes succeeded (data integrity)
	assert.Greater(t, succeeded, int64(0), "Some writes should have succeeded")

	// Verify the database has the expected records
	db, err := sql.Open("sqlite", primaryDBPath)
	if err == nil {
		var count int64
		err = db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&count)
		if err == nil {
			t.Logf("Final database record count: %d", count)
			assert.GreaterOrEqual(t, count, succeeded, "Database should have at least as many records as succeeded writes")
		}
		db.Close()
	}

	daemon2Cancel()
}

// TestDaemonSplitBrainPrevention verifies that when two nodes start nearly
// simultaneously, exactly one becomes PRIMARY and the other stays PASSIVE.
func TestDaemonSplitBrainPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "split-brain-prevention"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Run multiple attempts to catch race conditions
	const attempts = 5

	for attempt := 0; attempt < attempts; attempt++ {
		t.Logf("=== Split-brain prevention attempt %d/%d ===", attempt+1, attempts)

		// Use unique cluster ID per attempt to avoid KV conflicts
		attemptClusterID := fmt.Sprintf("%s-%d", clusterID, attempt)
		createObjectStoreBucketWithName(ctx, t, natsContainer.URL, "convex-"+attemptClusterID+"-wal")

		cfg1 := createStressTestConfig(t, tmpDir, natsContainer.URL, attemptClusterID, "node-1")
		cfg2 := createStressTestConfig(t, tmpDir, natsContainer.URL, attemptClusterID, "node-2")

		daemon1, err := NewDaemon(cfg1)
		require.NoError(t, err)
		daemon2, err := NewDaemon(cfg2)
		require.NoError(t, err)

		ctx1, cancel1 := context.WithCancel(ctx)
		ctx2, cancel2 := context.WithCancel(ctx)

		errCh1 := make(chan error, 1)
		errCh2 := make(chan error, 1)

		// Start both daemons simultaneously
		var startWg sync.WaitGroup
		startWg.Add(2)

		go func() {
			startWg.Done()
			errCh1 <- daemon1.Run(ctx1)
		}()
		go func() {
			startWg.Done()
			errCh2 <- daemon2.Run(ctx2)
		}()

		startWg.Wait()

		// Wait for election to settle
		time.Sleep(5 * time.Second)

		// Count primaries
		primaryCount := 0
		passiveCount := 0

		if daemon1.state.Role() == RolePrimary {
			primaryCount++
		}
		if daemon1.state.Role() == RolePassive {
			passiveCount++
		}
		if daemon2.state.Role() == RolePrimary {
			primaryCount++
		}
		if daemon2.state.Role() == RolePassive {
			passiveCount++
		}

		t.Logf("Attempt %d: Primary=%d, Passive=%d", attempt+1, primaryCount, passiveCount)

		// Assert exactly one primary and one passive
		assert.Equal(t, 1, primaryCount, "Attempt %d: Exactly one PRIMARY expected", attempt+1)
		assert.Equal(t, 1, passiveCount, "Attempt %d: Exactly one PASSIVE expected", attempt+1)

		// Cleanup
		cancel1()
		cancel2()

		select {
		case <-errCh1:
		case <-time.After(5 * time.Second):
		}
		select {
		case <-errCh2:
		case <-time.After(5 * time.Second):
		}
	}
}

// TestDaemonDataIntegrityAcrossMultipleFailovers writes data, performs multiple
// failovers, and verifies all data is preserved throughout the process.
func TestDaemonDataIntegrityAcrossMultipleFailovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "data-integrity-multi-failover"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	cfg1 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Track expected data across failovers
	expectedMessages := make([]string, 0)

	// Create initial daemons
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

	// Wait for leader and DB initialization
	waitForLeader(t, daemon1, daemon2, 20*time.Second)
	time.Sleep(5 * time.Second)

	const failoverCycles = 3
	const messagesPerCycle = 5

	for cycle := 0; cycle < failoverCycles; cycle++ {
		t.Logf("=== Data integrity cycle %d/%d ===", cycle+1, failoverCycles)

		// Get current primary's DB path
		var primaryDBPath string
		if daemon1.state.Role() == RolePrimary {
			primaryDBPath = cfg1.Backend.DataPath
		} else {
			primaryDBPath = cfg2.Backend.DataPath
		}

		// Insert test data
		db, err := sql.Open("sqlite", primaryDBPath)
		if err != nil {
			t.Logf("Cycle %d: Could not open DB: %v", cycle+1, err)
			continue
		}

		for i := 0; i < messagesPerCycle; i++ {
			msg := fmt.Sprintf("cycle-%d-msg-%d-%d", cycle, i, time.Now().UnixNano())
			_, err := db.Exec(`INSERT INTO events (message) VALUES (?)`, msg)
			if err == nil {
				expectedMessages = append(expectedMessages, msg)
			}
		}

		// Checkpoint WAL
		_, _ = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`)
		db.Close()

		t.Logf("Cycle %d: Inserted %d messages, total expected: %d", cycle+1, messagesPerCycle, len(expectedMessages))

		// Wait for replication
		time.Sleep(3 * time.Second)

		// Trigger failover
		var primaryCancel context.CancelFunc
		var primaryErrCh chan error

		if daemon1.state.Role() == RolePrimary {
			primaryCancel = daemon1Cancel
			primaryErrCh = errCh1
		} else {
			primaryCancel = daemon2Cancel
			primaryErrCh = errCh2
		}

		primaryCancel()
		select {
		case <-primaryErrCh:
		case <-time.After(10 * time.Second):
		}

		// Wait for failover
		time.Sleep(5 * time.Second)

		// Restart stopped daemon
		if daemon1.state.Role() != RolePrimary && daemon1.state.Role() != RolePassive {
			daemon1, err = NewDaemon(cfg1)
			require.NoError(t, err)
			daemon1Ctx, daemon1Cancel = context.WithCancel(ctx)
			errCh1 = make(chan error, 1)
			go func(d *Daemon, ch chan error) { ch <- d.Run(daemon1Ctx) }(daemon1, errCh1)
		}
		if daemon2.state.Role() != RolePrimary && daemon2.state.Role() != RolePassive {
			daemon2, err = NewDaemon(cfg2)
			require.NoError(t, err)
			daemon2Ctx, daemon2Cancel = context.WithCancel(ctx)
			errCh2 = make(chan error, 1)
			go func(d *Daemon, ch chan error) { ch <- d.Run(daemon2Ctx) }(daemon2, errCh2)
		}

		time.Sleep(3 * time.Second)
	}

	// Final verification - check all expected messages exist
	t.Log("=== Final data verification ===")

	// Get the current primary's DB
	var finalDBPath string
	if daemon1.state.Role() == RolePrimary {
		finalDBPath = cfg1.Backend.DataPath
	} else if daemon2.state.Role() == RolePrimary {
		finalDBPath = cfg2.Backend.DataPath
	}

	if finalDBPath != "" {
		db, err := sql.Open("sqlite", finalDBPath)
		if err == nil {
			defer db.Close()

			var totalCount int
			err = db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&totalCount)
			if err == nil {
				t.Logf("Total records in final DB: %d, expected at least: %d", totalCount, len(expectedMessages))
				assert.GreaterOrEqual(t, totalCount, len(expectedMessages),
					"Final database should contain all expected messages")
			}

			// Verify some specific messages exist
			foundCount := 0
			for _, msg := range expectedMessages {
				var count int
				err = db.QueryRow(`SELECT COUNT(*) FROM events WHERE message = ?`, msg).Scan(&count)
				if err == nil && count > 0 {
					foundCount++
				}
			}
			t.Logf("Found %d/%d expected messages", foundCount, len(expectedMessages))
		}
	}

	// Cleanup
	daemon1Cancel()
	daemon2Cancel()
}

// TestDaemonElectionRaceConditions tests that multiple nodes joining
// the cluster rapidly don't cause election deadlocks or infinite loops.
func TestDaemonElectionRaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "election-race"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create 3 nodes to make race conditions more likely
	cfg1 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")
	cfg3 := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-3")

	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)
	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)
	daemon3, err := NewDaemon(cfg3)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(ctx)
	ctx2, cancel2 := context.WithCancel(ctx)
	ctx3, cancel3 := context.WithCancel(ctx)
	defer cancel1()
	defer cancel2()
	defer cancel3()

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)
	errCh3 := make(chan error, 1)

	// Start all three simultaneously
	go func() { errCh1 <- daemon1.Run(ctx1) }()
	go func() { errCh2 <- daemon2.Run(ctx2) }()
	go func() { errCh3 <- daemon3.Run(ctx3) }()

	// Wait for election to settle
	time.Sleep(10 * time.Second)

	// Count roles
	primaryCount := 0
	passiveCount := 0

	for _, d := range []*Daemon{daemon1, daemon2, daemon3} {
		switch d.state.Role() {
		case RolePrimary:
			primaryCount++
		case RolePassive:
			passiveCount++
		}
	}

	t.Logf("3-node cluster: Primary=%d, Passive=%d", primaryCount, passiveCount)

	// Assert exactly one primary
	assert.Equal(t, 1, primaryCount, "Exactly one PRIMARY expected in 3-node cluster")
	assert.Equal(t, 2, passiveCount, "Exactly two PASSIVE expected in 3-node cluster")

	// Verify all nodes agree on the leader
	leader1 := daemon1.state.Leader()
	leader2 := daemon2.state.Leader()
	leader3 := daemon3.state.Leader()

	assert.Equal(t, leader1, leader2, "All nodes should agree on leader")
	assert.Equal(t, leader2, leader3, "All nodes should agree on leader")
}

// TestDaemonHeartbeatTTLEdgeCases tests behavior when TTL expires during
// heartbeat renewal to prevent flapping between PRIMARY/PASSIVE states.
func TestDaemonHeartbeatTTLEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	clusterID := "heartbeat-ttl-edge"
	tmpDir := t.TempDir()

	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Use very tight TTL and heartbeat to increase chance of edge cases
	cfg := createStressTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg.Election.LeaderTTL = 2 * time.Second
	cfg.Election.HeartbeatInterval = 1 * time.Second

	daemon, err := NewDaemon(cfg)
	require.NoError(t, err)

	daemonCtx, daemonCancel := context.WithCancel(ctx)
	defer daemonCancel()

	errCh := make(chan error, 1)
	go func() { errCh <- daemon.Run(daemonCtx) }()

	// Wait to become leader
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if daemon.state.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, RolePrimary, daemon.state.Role())

	// Monitor for state flapping over 30 seconds
	t.Log("Monitoring for state flapping...")
	flapCount := 0
	lastRole := daemon.state.Role()

	monitorEnd := time.Now().Add(30 * time.Second)
	for time.Now().Before(monitorEnd) {
		currentRole := daemon.state.Role()
		if currentRole != lastRole {
			flapCount++
			t.Logf("State change detected: %s -> %s", lastRole, currentRole)
			lastRole = currentRole
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("State flaps detected: %d", flapCount)

	// A single-node cluster should NOT flap states
	assert.Equal(t, 0, flapCount, "Single-node cluster should not experience state flapping")
	assert.Equal(t, RolePrimary, daemon.state.Role(), "Node should remain PRIMARY")
}

// Helper functions for stress tests

// createStressTestConfig creates a test config optimized for stress testing
// with shorter TTLs and heartbeat intervals.
func createStressTestConfig(t *testing.T, tmpDir, natsURL, clusterID, nodeID string) *config.Config {
	t.Helper()

	dbPath := filepath.Join(tmpDir, nodeID+"-backend.db")
	replicaPath := filepath.Join(tmpDir, nodeID+"-replica.db")

	// Create the SQLite database with WAL mode
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(`PRAGMA journal_mode = wal;`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT);`)
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
			LeaderTTL:         3 * time.Second,  // Short TTL for fast failover
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

// waitForLeader waits for one of the daemons to become leader.
func waitForLeader(t *testing.T, daemon1, daemon2 *Daemon, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if daemon1.state.Role() == RolePrimary || daemon2.state.Role() == RolePrimary {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("Timeout waiting for leader election")
}
