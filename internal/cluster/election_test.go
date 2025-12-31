package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"
)

func TestElection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	t.Run("single node becomes leader", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID:         "test-single-leader",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		election, err := NewElection(cfg)
		require.NoError(t, err)
		defer election.Stop()

		// Start election
		err = election.Start(ctx)
		require.NoError(t, err)

		// Wait for leadership
		select {
		case <-election.LeaderCh():
			assert.True(t, election.IsLeader())
			assert.Equal(t, "node-1", election.CurrentLeader())
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting to become leader")
		}
	})

	t.Run("first node wins election", func(t *testing.T) {
		cfg1 := ElectionConfig{
			ClusterID:         "test-first-wins",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		cfg2 := ElectionConfig{
			ClusterID:         "test-first-wins",
			NodeID:            "node-2",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		election1, err := NewElection(cfg1)
		require.NoError(t, err)
		defer election1.Stop()

		election2, err := NewElection(cfg2)
		require.NoError(t, err)
		defer election2.Stop()

		// Start first node
		err = election1.Start(ctx)
		require.NoError(t, err)

		// Wait for node 1 to become leader
		select {
		case <-election1.LeaderCh():
			assert.True(t, election1.IsLeader())
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for node-1 to become leader")
		}

		// Start second node
		err = election2.Start(ctx)
		require.NoError(t, err)

		// Give node 2 time to detect leader via watcher
		time.Sleep(2 * time.Second)

		// Node 2 should not be leader
		assert.False(t, election2.IsLeader())
		assert.Equal(t, "node-1", election2.CurrentLeader())
	})

	t.Run("failover when leader stops", func(t *testing.T) {
		cfg1 := ElectionConfig{
			ClusterID:         "test-failover",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         3 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		cfg2 := ElectionConfig{
			ClusterID:         "test-failover",
			NodeID:            "node-2",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         3 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		election1, err := NewElection(cfg1)
		require.NoError(t, err)

		election2, err := NewElection(cfg2)
		require.NoError(t, err)
		defer election2.Stop()

		// Start both nodes
		err = election1.Start(ctx)
		require.NoError(t, err)

		// Wait for node 1 to become leader
		select {
		case <-election1.LeaderCh():
			// Node 1 is leader
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for node-1 to become leader")
		}

		err = election2.Start(ctx)
		require.NoError(t, err)

		// Stop node 1 (simulating failure)
		election1.Stop()

		// Wait for node 2 to become leader (should happen after TTL expires)
		// The watcher should detect the key deletion
		select {
		case <-election2.LeaderCh():
			assert.True(t, election2.IsLeader())
			assert.Equal(t, "node-2", election2.CurrentLeader())
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for node-2 to become leader after failover")
		}
	})

	t.Run("leader steps down gracefully", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID:         "test-stepdown",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		election, err := NewElection(cfg)
		require.NoError(t, err)
		defer election.Stop()

		err = election.Start(ctx)
		require.NoError(t, err)

		// Wait for leadership
		select {
		case <-election.LeaderCh():
			assert.True(t, election.IsLeader())
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting to become leader")
		}

		// Step down
		err = election.StepDown(ctx)
		require.NoError(t, err)

		// Verify node is in cooldown and won't immediately re-acquire
		assert.False(t, election.IsLeader())

		// Wait a short time and verify still not leader (cooldown active)
		time.Sleep(500 * time.Millisecond)
		assert.False(t, election.IsLeader())
	})

	t.Run("reclaims leadership when leader key contains own node ID", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID:         "test-reclaim",
			NodeID:            "test-node",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 100 * time.Millisecond,
		}

		election, err := NewElection(cfg)
		require.NoError(t, err)
		defer election.Stop()

		err = election.Start(ctx)
		require.NoError(t, err)

		// Wait to become leader
		select {
		case <-election.LeaderCh():
			assert.True(t, election.IsLeader())
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting to become leader")
		}
		assert.Equal(t, "test-node", election.CurrentLeader())

		// Simulate a failed renewal by manually clearing isLeader but keeping the KV key
		election.mu.Lock()
		election.isLeader = false
		election.revision = 0
		election.mu.Unlock()

		// Now isLeader is false but the KV key still contains our node ID
		assert.False(t, election.IsLeader())
		assert.Equal(t, "test-node", election.CurrentLeader())

		// Wait for the next heartbeat to reclaim leadership
		time.Sleep(200 * time.Millisecond)

		// Should have reclaimed leadership
		assert.True(t, election.IsLeader())
		assert.Equal(t, "test-node", election.CurrentLeader())
	})

	t.Run("heartbeat renews leadership", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID:         "test-heartbeat",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         3 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		election, err := NewElection(cfg)
		require.NoError(t, err)
		defer election.Stop()

		err = election.Start(ctx)
		require.NoError(t, err)

		// Wait for leadership
		select {
		case <-election.LeaderCh():
			assert.True(t, election.IsLeader())
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting to become leader")
		}

		// Wait longer than TTL - heartbeat should keep us as leader
		time.Sleep(5 * time.Second)

		assert.True(t, election.IsLeader())
	})

	t.Run("watcher detects leader change in real-time", func(t *testing.T) {
		cfg1 := ElectionConfig{
			ClusterID:         "test-watcher-realtime",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         10 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		cfg2 := ElectionConfig{
			ClusterID:         "test-watcher-realtime",
			NodeID:            "node-2",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         10 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		election1, err := NewElection(cfg1)
		require.NoError(t, err)
		defer election1.Stop()

		election2, err := NewElection(cfg2)
		require.NoError(t, err)
		defer election2.Stop()

		// Start node 1 first
		err = election1.Start(ctx)
		require.NoError(t, err)

		// Wait for node 1 to become leader
		select {
		case <-election1.LeaderCh():
			assert.True(t, election1.IsLeader())
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for node-1 to become leader")
		}

		// Start node 2
		err = election2.Start(ctx)
		require.NoError(t, err)

		// Give watcher time to detect leader
		time.Sleep(500 * time.Millisecond)

		// Node 2 should see node-1 as leader immediately via watcher
		assert.False(t, election2.IsLeader())
		assert.Equal(t, "node-1", election2.CurrentLeader())

		// Node 1 steps down
		err = election1.StepDown(ctx)
		require.NoError(t, err)

		// Node 2 should detect the change via watcher and acquire leadership
		select {
		case <-election2.LeaderCh():
			assert.True(t, election2.IsLeader())
			assert.Equal(t, "node-2", election2.CurrentLeader())
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for node-2 to become leader after step down")
		}
	})

	t.Run("bucket name is auth-compatible", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID:         "my-cluster",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		// Verify bucket name format
		assert.Equal(t, "convex-my-cluster-election", cfg.BucketName())

		election, err := NewElection(cfg)
		require.NoError(t, err)
		defer election.Stop()

		err = election.Start(ctx)
		require.NoError(t, err)

		// Verify bucket was created with correct name
		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		js, err := jetstream.New(nc)
		require.NoError(t, err)

		kv, err := js.KeyValue(ctx, "convex-my-cluster-election")
		require.NoError(t, err)
		assert.NotNil(t, kv)
	})
}

func TestElectionConfig(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		cfg := ElectionConfig{}
		_, err := NewElection(cfg)
		assert.Error(t, err)
	})

	t.Run("validates NATS URLs required", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID: "cluster",
			NodeID:    "node",
		}
		_, err := NewElection(cfg)
		assert.Error(t, err)
	})

	t.Run("applies defaults", func(t *testing.T) {
		cfg := ElectionConfig{
			ClusterID: "cluster",
			NodeID:    "node",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		election, err := NewElection(cfg)
		require.NoError(t, err)

		// Verify defaults were applied
		assert.Equal(t, 10*time.Second, election.cfg.LeaderTTL)
		assert.Equal(t, 3*time.Second, election.cfg.HeartbeatInterval)
	})

	t.Run("bucket name format", func(t *testing.T) {
		testCases := []struct {
			clusterID    string
			expectedName string
		}{
			{"my-cluster", "convex-my-cluster-election"},
			{"prod", "convex-prod-election"},
			{"test-env-1", "convex-test-env-1-election"},
		}

		for _, tc := range testCases {
			cfg := ElectionConfig{ClusterID: tc.clusterID}
			assert.Equal(t, tc.expectedName, cfg.BucketName())
		}
	})
}

func TestElectionErrors(t *testing.T) {
	t.Run("error types are defined", func(t *testing.T) {
		assert.NotNil(t, ErrElectionClosed)
	})
}
