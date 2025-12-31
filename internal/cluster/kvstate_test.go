package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"
)

func TestStateManager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	t.Run("starts and stops", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-start-stop",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		assert.True(t, sm.Running())

		err = sm.Stop()
		require.NoError(t, err)
		assert.False(t, sm.Running())
	})

	t.Run("set and get node state", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-set-get",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		// Set state
		state := &NodeState{
			NodeID:        "node-1",
			ClusterID:     "test-set-get",
			Role:          "PRIMARY",
			BackendStatus: "running",
		}
		err = sm.SetNodeState(ctx, state)
		require.NoError(t, err)

		// Get state
		retrieved, err := sm.GetNodeState(ctx, "node-1")
		require.NoError(t, err)
		assert.Equal(t, "node-1", retrieved.NodeID)
		assert.Equal(t, "test-set-get", retrieved.ClusterID)
		assert.Equal(t, "PRIMARY", retrieved.Role)
		assert.Equal(t, "running", retrieved.BackendStatus)
		assert.False(t, retrieved.LastSeen.IsZero())
	})

	t.Run("get non-existent node returns error", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-nonexistent",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		_, err = sm.GetNodeState(ctx, "nonexistent-node")
		assert.ErrorIs(t, err, ErrNodeNotFound)
	})

	t.Run("delete node state", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-delete",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		// Set state
		state := &NodeState{
			NodeID:        "node-1",
			ClusterID:     "test-delete",
			Role:          "PRIMARY",
			BackendStatus: "running",
		}
		err = sm.SetNodeState(ctx, state)
		require.NoError(t, err)

		// Verify it exists
		_, err = sm.GetNodeState(ctx, "node-1")
		require.NoError(t, err)

		// Delete state
		err = sm.DeleteNodeState(ctx, "node-1")
		require.NoError(t, err)

		// Verify it's gone
		_, err = sm.GetNodeState(ctx, "node-1")
		assert.ErrorIs(t, err, ErrNodeNotFound)
	})

	t.Run("list nodes", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-list",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		// Set multiple node states
		for i, nodeID := range []string{"node-1", "node-2", "node-3"} {
			state := &NodeState{
				NodeID:        nodeID,
				ClusterID:     "test-list",
				Role:          "PASSIVE",
				BackendStatus: "stopped",
			}
			if i == 0 {
				state.Role = "PRIMARY"
				state.BackendStatus = "running"
			}
			err = sm.SetNodeState(ctx, state)
			require.NoError(t, err)
		}

		// List nodes
		nodes, err := sm.ListNodes(ctx)
		require.NoError(t, err)
		assert.Len(t, nodes, 3)
		assert.Contains(t, nodes, "node-1")
		assert.Contains(t, nodes, "node-2")
		assert.Contains(t, nodes, "node-3")
		assert.Equal(t, "PRIMARY", nodes["node-1"].Role)
		assert.Equal(t, "PASSIVE", nodes["node-2"].Role)
	})

	t.Run("watch nodes receives updates", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-watch-all",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		// Start watching
		watchCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		events, err := sm.WatchNodes(watchCtx)
		require.NoError(t, err)

		// Give watcher time to start
		time.Sleep(200 * time.Millisecond)

		// Set a node state
		state := &NodeState{
			NodeID:        "node-1",
			ClusterID:     "test-watch-all",
			Role:          "PRIMARY",
			BackendStatus: "running",
		}
		err = sm.SetNodeState(ctx, state)
		require.NoError(t, err)

		// Receive event
		select {
		case event := <-events:
			assert.Equal(t, "node-1", event.NodeID)
			assert.Equal(t, "put", event.Operation)
			assert.NotNil(t, event.State)
			assert.Equal(t, "PRIMARY", event.State.Role)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for watch event")
		}

		// Update state
		state.Role = "PASSIVE"
		err = sm.SetNodeState(ctx, state)
		require.NoError(t, err)

		// Receive update event
		select {
		case event := <-events:
			assert.Equal(t, "node-1", event.NodeID)
			assert.Equal(t, "put", event.Operation)
			assert.Equal(t, "PASSIVE", event.State.Role)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for update event")
		}

		// Delete state
		err = sm.DeleteNodeState(ctx, "node-1")
		require.NoError(t, err)

		// Receive delete event
		select {
		case event := <-events:
			assert.Equal(t, "node-1", event.NodeID)
			assert.Equal(t, "delete", event.Operation)
			assert.Nil(t, event.State)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for delete event")
		}
	})

	t.Run("watch specific node", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-watch-node",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		// Start watching node-1
		watchCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		events, err := sm.WatchNode(watchCtx, "node-1")
		require.NoError(t, err)

		// Give watcher time to start
		time.Sleep(200 * time.Millisecond)

		// Set state for node-1
		state := &NodeState{
			NodeID:        "node-1",
			ClusterID:     "test-watch-node",
			Role:          "PRIMARY",
			BackendStatus: "running",
		}
		err = sm.SetNodeState(ctx, state)
		require.NoError(t, err)

		// Should receive event for node-1
		select {
		case event := <-events:
			assert.Equal(t, "node-1", event.NodeID)
			assert.Equal(t, "put", event.Operation)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for node-1 event")
		}

		// Set state for node-2 (should NOT trigger our watcher)
		state2 := &NodeState{
			NodeID:        "node-2",
			ClusterID:     "test-watch-node",
			Role:          "PASSIVE",
			BackendStatus: "stopped",
		}
		err = sm.SetNodeState(ctx, state2)
		require.NoError(t, err)

		// Should NOT receive event for node-2
		select {
		case event := <-events:
			t.Fatalf("should not receive event for node-2, got: %+v", event)
		case <-time.After(500 * time.Millisecond):
			// Good - no event received
		}
	})

	t.Run("update own state convenience method", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "test-update-own",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm, err := NewStateManager(cfg)
		require.NoError(t, err)

		err = sm.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm.Stop() }()

		// Update own state
		err = sm.UpdateOwnState(ctx, "PRIMARY", "running")
		require.NoError(t, err)

		// Verify
		state, err := sm.GetNodeState(ctx, "node-1")
		require.NoError(t, err)
		assert.Equal(t, "node-1", state.NodeID)
		assert.Equal(t, "test-update-own", state.ClusterID)
		assert.Equal(t, "PRIMARY", state.Role)
		assert.Equal(t, "running", state.BackendStatus)
	})

	t.Run("multiple state managers can share state", func(t *testing.T) {
		cfg1 := StateConfig{
			ClusterID: "test-shared",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}
		cfg2 := StateConfig{
			ClusterID: "test-shared",
			NodeID:    "node-2",
			NATSURLs:  []string{natsContainer.URL},
		}

		sm1, err := NewStateManager(cfg1)
		require.NoError(t, err)
		sm2, err := NewStateManager(cfg2)
		require.NoError(t, err)

		err = sm1.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm1.Stop() }()

		err = sm2.Start(ctx)
		require.NoError(t, err)
		defer func() { _ = sm2.Stop() }()

		// sm1 sets node-1 state
		err = sm1.UpdateOwnState(ctx, "PRIMARY", "running")
		require.NoError(t, err)

		// sm2 sets node-2 state
		err = sm2.UpdateOwnState(ctx, "PASSIVE", "stopped")
		require.NoError(t, err)

		// Both can see both nodes
		state1, err := sm2.GetNodeState(ctx, "node-1")
		require.NoError(t, err)
		assert.Equal(t, "PRIMARY", state1.Role)

		state2, err := sm1.GetNodeState(ctx, "node-2")
		require.NoError(t, err)
		assert.Equal(t, "PASSIVE", state2.Role)

		// Both can list all nodes
		nodes1, err := sm1.ListNodes(ctx)
		require.NoError(t, err)
		assert.Len(t, nodes1, 2)

		nodes2, err := sm2.ListNodes(ctx)
		require.NoError(t, err)
		assert.Len(t, nodes2, 2)
	})
}

func TestStateConfig(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		cfg := StateConfig{}
		_, err := NewStateManager(cfg)
		assert.Error(t, err)
	})

	t.Run("validates cluster ID required", func(t *testing.T) {
		cfg := StateConfig{
			NodeID:   "node",
			NATSURLs: []string{"nats://localhost:4222"},
		}
		_, err := NewStateManager(cfg)
		assert.Error(t, err)
	})

	t.Run("validates node ID required", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "cluster",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		_, err := NewStateManager(cfg)
		assert.Error(t, err)
	})

	t.Run("validates NATS URLs required", func(t *testing.T) {
		cfg := StateConfig{
			ClusterID: "cluster",
			NodeID:    "node",
		}
		_, err := NewStateManager(cfg)
		assert.Error(t, err)
	})

	t.Run("bucket name format", func(t *testing.T) {
		testCases := []struct {
			clusterID    string
			expectedName string
		}{
			{"my-cluster", "convex-my-cluster-state"},
			{"prod", "convex-prod-state"},
			{"test-env-1", "convex-test-env-1-state"},
		}

		for _, tc := range testCases {
			cfg := StateConfig{ClusterID: tc.clusterID}
			assert.Equal(t, tc.expectedName, cfg.BucketName())
		}
	})

	t.Run("node key format", func(t *testing.T) {
		testCases := []struct {
			nodeID      string
			expectedKey string
		}{
			{"node-1", "nodes.node-1"},
			{"server-a", "nodes.server-a"},
			{"my-node", "nodes.my-node"},
		}

		for _, tc := range testCases {
			cfg := StateConfig{}
			assert.Equal(t, tc.expectedKey, cfg.NodeKey(tc.nodeID))
		}
	})
}

func TestNodeState(t *testing.T) {
	t.Run("json marshaling", func(t *testing.T) {
		now := time.Now().Truncate(time.Millisecond)
		state := NodeState{
			NodeID:        "node-1",
			ClusterID:     "my-cluster",
			Role:          "PRIMARY",
			BackendStatus: "running",
			LastSeen:      now,
			Version:       "1.0.0",
		}

		data, err := json.Marshal(state)
		require.NoError(t, err)

		var decoded NodeState
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, state.NodeID, decoded.NodeID)
		assert.Equal(t, state.ClusterID, decoded.ClusterID)
		assert.Equal(t, state.Role, decoded.Role)
		assert.Equal(t, state.BackendStatus, decoded.BackendStatus)
		assert.Equal(t, state.Version, decoded.Version)
	})
}

func TestStateEvent(t *testing.T) {
	t.Run("represents put event", func(t *testing.T) {
		state := &NodeState{
			NodeID: "node-1",
			Role:   "PRIMARY",
		}
		event := StateEvent{
			NodeID:    "node-1",
			State:     state,
			Operation: "put",
		}

		assert.Equal(t, "node-1", event.NodeID)
		assert.NotNil(t, event.State)
		assert.Equal(t, "put", event.Operation)
	})

	t.Run("represents delete event", func(t *testing.T) {
		event := StateEvent{
			NodeID:    "node-1",
			State:     nil,
			Operation: "delete",
		}

		assert.Equal(t, "node-1", event.NodeID)
		assert.Nil(t, event.State)
		assert.Equal(t, "delete", event.Operation)
	})
}

func TestStateErrors(t *testing.T) {
	t.Run("error types are defined", func(t *testing.T) {
		assert.NotNil(t, ErrStateNotStarted)
		assert.NotNil(t, ErrNodeNotFound)
	})
}
