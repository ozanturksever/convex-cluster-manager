package cluster

import (
	"context"
	"testing"
	"time"

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
			ClusterID:         "test-cluster",
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
			ClusterID:         "test-cluster-2",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         5 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		cfg2 := ElectionConfig{
			ClusterID:         "test-cluster-2",
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

		// Give node 2 time to detect leader
		time.Sleep(2 * time.Second)

		// Node 2 should not be leader
		assert.False(t, election2.IsLeader())
		assert.Equal(t, "node-1", election2.CurrentLeader())
	})

	t.Run("failover when leader stops", func(t *testing.T) {
		cfg1 := ElectionConfig{
			ClusterID:         "test-cluster-3",
			NodeID:            "node-1",
			NATSURLs:          []string{natsContainer.URL},
			LeaderTTL:         3 * time.Second,
			HeartbeatInterval: 1 * time.Second,
		}

		cfg2 := ElectionConfig{
			ClusterID:         "test-cluster-3",
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
			ClusterID:         "test-cluster-4",
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
			ClusterID:         "test-cluster-reclaim",
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
		// This simulates the race condition where renewLeadership fails
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
			ClusterID:         "test-cluster-5",
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
}
