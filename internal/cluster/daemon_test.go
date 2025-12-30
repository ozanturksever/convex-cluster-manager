package cluster

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/internal/config"
	"github.com/ozanturksever/convex-cluster-manager/internal/health"
	"github.com/ozanturksever/convex-cluster-manager/testutil"

	_ "modernc.org/sqlite"
)

// createTestConfig creates a test configuration for daemon tests.
// It sets up paths in the provided temp directory and uses the provided NATS URL.
func createTestConfig(t *testing.T, tmpDir, natsURL, clusterID, nodeID string) *config.Config {
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

	cfg := &config.Config{
		ClusterID: clusterID,
		NodeID:    nodeID,
		NATS: config.NATSConfig{
			Servers: []string{natsURL},
		},
		Election: config.ElectionConfig{
			LeaderTTL:         3 * time.Second,
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

	return cfg
}

// createObjectStoreBucket creates the NATS Object Store bucket for WAL replication.
func createObjectStoreBucket(ctx context.Context, t *testing.T, natsURL, clusterID string) {
	t.Helper()

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	bucketName := "convex." + clusterID + ".wal"
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucketName})
	if err != nil {
		// Bucket may already exist, try to get it
		_, err = js.ObjectStore(ctx, bucketName)
		require.NoError(t, err)
	}
}

func TestDaemonSingleNodeBecomesLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	clusterID := "daemon-single-node"
	tmpDir := t.TempDir()

	// Create Object Store bucket for WAL replication
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create test config
	cfg := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")

	// Create daemon
	daemon, err := NewDaemon(cfg)
	require.NoError(t, err)

	// Run daemon in background
	daemonCtx, daemonCancel := context.WithCancel(ctx)
	defer daemonCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- daemon.Run(daemonCtx)
	}()

	// Wait for the node to become leader
	deadline := time.Now().Add(15 * time.Second)
	var becameLeader bool
	for time.Now().Before(deadline) {
		if daemon.state.Role() == RolePrimary {
			becameLeader = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, becameLeader, "single node should become leader")
	assert.Equal(t, "node-1", daemon.state.Leader())

	// Shutdown daemon
	daemonCancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon to stop")
	}
}

func TestDaemonTwoNodeFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	clusterID := "daemon-two-node-failover"
	tmpDir := t.TempDir()

	// Create Object Store bucket for WAL replication
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create test configs for two nodes
	cfg1 := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Create daemons
	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	// Run daemon 1 in background
	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- daemon1.Run(daemon1Ctx)
	}()

	// Wait for node 1 to become leader
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if daemon1.state.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, RolePrimary, daemon1.state.Role(), "node-1 should become leader first")

	// Run daemon 2 in background
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- daemon2.Run(daemon2Ctx)
	}()

	// Give daemon 2 time to start and detect the leader
	time.Sleep(2 * time.Second)

	// Verify node 2 is passive and recognizes node 1 as leader
	assert.Equal(t, RolePassive, daemon2.state.Role(), "node-2 should be passive")
	assert.Equal(t, "node-1", daemon2.state.Leader(), "node-2 should recognize node-1 as leader")

	// Stop daemon 1 to simulate failure
	daemon1Cancel()

	select {
	case err := <-errCh1:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon1 to stop")
	}

	// Wait for node 2 to become leader after TTL expiration
	deadline = time.Now().Add(20 * time.Second)
	var node2BecameLeader bool
	for time.Now().Before(deadline) {
		if daemon2.state.Role() == RolePrimary {
			node2BecameLeader = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, node2BecameLeader, "node-2 should become leader after node-1 fails")
	assert.Equal(t, "node-2", daemon2.state.Leader(), "node-2 should be its own leader")

	// Shutdown daemon 2
	daemon2Cancel()

	select {
	case err := <-errCh2:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon2 to stop")
	}
}

func TestDaemonGracefulStepdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	clusterID := "daemon-graceful-stepdown"
	tmpDir := t.TempDir()

	// Create Object Store bucket for WAL replication
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create test configs for two nodes
	cfg1 := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Create daemons
	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	// Run daemon 1 in background
	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	defer daemon1Cancel()

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- daemon1.Run(daemon1Ctx)
	}()

	// Wait for node 1 to become leader
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if daemon1.state.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, RolePrimary, daemon1.state.Role(), "node-1 should become leader first")

	// Run daemon 2 in background
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- daemon2.Run(daemon2Ctx)
	}()

	// Give daemon 2 time to start
	time.Sleep(2 * time.Second)
	require.Equal(t, RolePassive, daemon2.state.Role(), "node-2 should be passive")

	// Have node 1 gracefully step down via election
	err = daemon1.election.StepDown(ctx)
	require.NoError(t, err)

	// Wait for node 2 to become leader
	deadline = time.Now().Add(15 * time.Second)
	var node2BecameLeader bool
	for time.Now().Before(deadline) {
		if daemon2.state.Role() == RolePrimary {
			node2BecameLeader = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, node2BecameLeader, "node-2 should become leader after node-1 steps down")

	// Verify node 1 transitioned to passive
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if daemon1.state.Role() == RolePassive {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, RolePassive, daemon1.state.Role(), "node-1 should be passive after stepping down")

	// Shutdown both daemons
	daemon1Cancel()
	daemon2Cancel()

	select {
	case <-errCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon1 to stop")
	}

	select {
	case <-errCh2:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon2 to stop")
	}
}

func TestDaemonHealthCheckerCrossNodeQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	clusterID := "daemon-health-query"
	tmpDir := t.TempDir()

	// Create Object Store bucket for WAL replication
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create test configs for two nodes
	cfg1 := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")
	cfg2 := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-2")

	// Create daemons
	daemon1, err := NewDaemon(cfg1)
	require.NoError(t, err)

	daemon2, err := NewDaemon(cfg2)
	require.NoError(t, err)

	// Run daemon 1 in background
	daemon1Ctx, daemon1Cancel := context.WithCancel(ctx)
	defer daemon1Cancel()

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- daemon1.Run(daemon1Ctx)
	}()

	// Wait for node 1 to become leader
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if daemon1.state.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, RolePrimary, daemon1.state.Role(), "node-1 should become leader")

	// Run daemon 2 in background
	daemon2Ctx, daemon2Cancel := context.WithCancel(ctx)
	defer daemon2Cancel()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- daemon2.Run(daemon2Ctx)
	}()

	// Give daemon 2 time to start and set up health checker
	time.Sleep(2 * time.Second)

	// Create a separate health checker client to query both nodes
	clientCfg := health.Config{
		ClusterID: clusterID,
		NodeID:    "query-client",
		NATSURLs:  []string{natsContainer.URL},
	}
	client, err := health.NewChecker(clientCfg)
	require.NoError(t, err)

	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop()

	// Query node 1's health
	resp1, err := client.QueryNode(ctx, "node-1", 5*time.Second)
	require.NoError(t, err)

	assert.Equal(t, "node-1", resp1.NodeID)
	assert.Equal(t, "PRIMARY", resp1.Role)
	assert.Equal(t, "node-1", resp1.Leader)
	assert.NotZero(t, resp1.Timestamp)

	// Query node 2's health
	resp2, err := client.QueryNode(ctx, "node-2", 5*time.Second)
	require.NoError(t, err)

	assert.Equal(t, "node-2", resp2.NodeID)
	assert.Equal(t, "PASSIVE", resp2.Role)
	assert.Equal(t, "node-1", resp2.Leader)
	assert.NotZero(t, resp2.Timestamp)

	// Shutdown both daemons
	daemon1Cancel()
	daemon2Cancel()

	select {
	case <-errCh1:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon1 to stop")
	}

	select {
	case <-errCh2:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon2 to stop")
	}
}

func TestDaemonRoleTransitionsUpdateHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer natsContainer.Stop(ctx)

	clusterID := "daemon-role-transitions"
	tmpDir := t.TempDir()

	// Create Object Store bucket for WAL replication
	createObjectStoreBucket(ctx, t, natsContainer.URL, clusterID)

	// Create test config
	cfg := createTestConfig(t, tmpDir, natsContainer.URL, clusterID, "node-1")

	// Create daemon
	daemon, err := NewDaemon(cfg)
	require.NoError(t, err)

	// Run daemon in background
	daemonCtx, daemonCancel := context.WithCancel(ctx)
	defer daemonCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- daemon.Run(daemonCtx)
	}()

	// Wait for the node to become leader
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if daemon.state.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, RolePrimary, daemon.state.Role())

	// Create a client to query health
	clientCfg := health.Config{
		ClusterID: clusterID,
		NodeID:    "query-client",
		NATSURLs:  []string{natsContainer.URL},
	}
	client, err := health.NewChecker(clientCfg)
	require.NoError(t, err)

	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop()

	// Query health when PRIMARY
	resp, err := client.QueryNode(ctx, "node-1", 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "PRIMARY", resp.Role)
	assert.Equal(t, "node-1", resp.Leader)

	// Step down from leadership
	err = daemon.election.StepDown(ctx)
	require.NoError(t, err)

	// Wait for state to transition to PASSIVE
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if daemon.state.Role() == RolePassive {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Give health checker time to update
	time.Sleep(500 * time.Millisecond)

	// Query health when PASSIVE
	resp, err = client.QueryNode(ctx, "node-1", 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "PASSIVE", resp.Role)

	// Shutdown daemon
	daemonCancel()

	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for daemon to stop")
	}
}

func TestNewDaemonValidation(t *testing.T) {
	t.Run("nil config returns error", func(t *testing.T) {
		_, err := NewDaemon(nil)
		assert.Error(t, err)
	})

	t.Run("valid config creates daemon", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		// Create a test database file
		db, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)
		_, err = db.Exec(`PRAGMA journal_mode = wal;`)
		require.NoError(t, err)
		err = db.Close()
		require.NoError(t, err)

		cfg := &config.Config{
			ClusterID: "test-cluster",
			NodeID:    "node-1",
			NATS: config.NATSConfig{
				Servers: []string{"nats://localhost:4222"},
			},
			Election: config.ElectionConfig{
				LeaderTTL:         10 * time.Second,
				HeartbeatInterval: 3 * time.Second,
			},
			WAL: config.WALConfig{
				StreamRetention:  24 * time.Hour,
				SnapshotInterval: time.Hour,
				ReplicaPath:      filepath.Join(tmpDir, "replica.db"),
			},
			Backend: config.BackendConfig{
				ServiceName:    "test-backend",
				HealthEndpoint: "http://localhost:3210/version",
				DataPath:       dbPath,
			},
		}

		daemon, err := NewDaemon(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, daemon)
		assert.NotNil(t, daemon.state)
		assert.NotNil(t, daemon.election)
		assert.NotNil(t, daemon.backend)
		assert.NotNil(t, daemon.primary)
		assert.NotNil(t, daemon.passive)
		assert.NotNil(t, daemon.health)
	})
}
