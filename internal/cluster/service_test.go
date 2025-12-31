package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"
)

// mockStatusProvider implements StatusProvider for testing.
type mockStatusProvider struct {
	status NodeStatus
}

func (m *mockStatusProvider) GetStatus() NodeStatus {
	return m.status
}

func TestService(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	t.Run("service starts and stops", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "test-cluster",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		svc, err := NewService(cfg)
		require.NoError(t, err)

		err = svc.Start(ctx)
		require.NoError(t, err)
		assert.True(t, svc.Running())

		err = svc.Stop()
		require.NoError(t, err)
		assert.False(t, svc.Running())
	})

	t.Run("service responds to status requests", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "test-status-cluster",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		svc, err := NewService(cfg)
		require.NoError(t, err)

		// Set mock status provider
		provider := &mockStatusProvider{
			status: NodeStatus{
				NodeID:        "node-1",
				ClusterID:     "test-status-cluster",
				Role:          "PRIMARY",
				Leader:        "node-1",
				BackendStatus: "running",
			},
		}
		svc.SetStatusProvider(provider)

		err = svc.Start(ctx)
		require.NoError(t, err)
		defer svc.Stop()

		// Connect and query status
		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		// Query the status endpoint
		subject := "convex.test-status-cluster.svc.node-1.status"
		msg, err := nc.Request(subject, nil, 5*time.Second)
		require.NoError(t, err)

		var status NodeStatus
		err = json.Unmarshal(msg.Data, &status)
		require.NoError(t, err)

		assert.Equal(t, "node-1", status.NodeID)
		assert.Equal(t, "test-status-cluster", status.ClusterID)
		assert.Equal(t, "PRIMARY", status.Role)
		assert.Equal(t, "node-1", status.Leader)
		assert.Equal(t, "running", status.BackendStatus)
		assert.Greater(t, status.Timestamp, int64(0))
		assert.GreaterOrEqual(t, status.UptimeMs, int64(0))
	})

	t.Run("service returns default status without provider", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "test-default-status",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		svc, err := NewService(cfg)
		require.NoError(t, err)

		// Don't set status provider

		err = svc.Start(ctx)
		require.NoError(t, err)
		defer svc.Stop()

		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		subject := "convex.test-default-status.svc.node-1.status"
		msg, err := nc.Request(subject, nil, 5*time.Second)
		require.NoError(t, err)

		var status NodeStatus
		err = json.Unmarshal(msg.Data, &status)
		require.NoError(t, err)

		assert.Equal(t, "node-1", status.NodeID)
		assert.Equal(t, "test-default-status", status.ClusterID)
		assert.Equal(t, "UNKNOWN", status.Role)
	})

	t.Run("query node status between services", func(t *testing.T) {
		cfg1 := ServiceConfig{
			ClusterID: "test-query-cluster",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		cfg2 := ServiceConfig{
			ClusterID: "test-query-cluster",
			NodeID:    "node-2",
			NATSURLs:  []string{natsContainer.URL},
		}

		svc1, err := NewService(cfg1)
		require.NoError(t, err)
		svc1.SetStatusProvider(&mockStatusProvider{
			status: NodeStatus{
				NodeID:    "node-1",
				ClusterID: "test-query-cluster",
				Role:      "PRIMARY",
				Leader:    "node-1",
			},
		})

		svc2, err := NewService(cfg2)
		require.NoError(t, err)
		svc2.SetStatusProvider(&mockStatusProvider{
			status: NodeStatus{
				NodeID:    "node-2",
				ClusterID: "test-query-cluster",
				Role:      "PASSIVE",
				Leader:    "node-1",
			},
		})

		err = svc1.Start(ctx)
		require.NoError(t, err)
		defer svc1.Stop()

		err = svc2.Start(ctx)
		require.NoError(t, err)
		defer svc2.Stop()

		// Node 2 queries node 1's status
		status, err := svc2.QueryNodeStatus(ctx, "node-1", 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "node-1", status.NodeID)
		assert.Equal(t, "PRIMARY", status.Role)

		// Node 1 queries node 2's status
		status, err = svc1.QueryNodeStatus(ctx, "node-2", 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "node-2", status.NodeID)
		assert.Equal(t, "PASSIVE", status.Role)
	})

	t.Run("service info available", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "test-info-cluster",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
			Version:   "2.0.0",
		}

		svc, err := NewService(cfg)
		require.NoError(t, err)

		err = svc.Start(ctx)
		require.NoError(t, err)
		defer svc.Stop()

		info := svc.Info()
		require.NotNil(t, info)
		assert.Equal(t, "convex-test-info-cluster-node-1", info.Name)
		assert.Equal(t, "2.0.0", info.Version)
		assert.Equal(t, "test-info-cluster", info.Metadata["cluster_id"])
		assert.Equal(t, "node-1", info.Metadata["node_id"])
	})

	t.Run("service stats available", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "test-stats-cluster",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		svc, err := NewService(cfg)
		require.NoError(t, err)

		err = svc.Start(ctx)
		require.NoError(t, err)
		defer svc.Stop()

		stats := svc.Stats()
		require.NotNil(t, stats)
		assert.Equal(t, "convex-test-stats-cluster-node-1", stats.Name)
	})
}

func TestServiceConfig(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		cfg := ServiceConfig{}
		_, err := NewService(cfg)
		assert.Error(t, err)
	})

	t.Run("validates cluster ID required", func(t *testing.T) {
		cfg := ServiceConfig{
			NodeID:   "node",
			NATSURLs: []string{"nats://localhost:4222"},
		}
		_, err := NewService(cfg)
		assert.Error(t, err)
	})

	t.Run("validates node ID required", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "cluster",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		_, err := NewService(cfg)
		assert.Error(t, err)
	})

	t.Run("validates NATS URLs required", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "cluster",
			NodeID:    "node",
		}
		_, err := NewService(cfg)
		assert.Error(t, err)
	})

	t.Run("applies default version", func(t *testing.T) {
		cfg := ServiceConfig{
			ClusterID: "cluster",
			NodeID:    "node",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		svc, err := NewService(cfg)
		require.NoError(t, err)
		assert.Equal(t, "1.0.0", svc.cfg.Version)
	})

	t.Run("subject base format", func(t *testing.T) {
		testCases := []struct {
			clusterID    string
			nodeID       string
			expectedBase string
		}{
			{"my-cluster", "node-1", "convex.my-cluster.svc.node-1"},
			{"prod", "server-a", "convex.prod.svc.server-a"},
			{"test-env", "node1", "convex.test-env.svc.node1"},
		}

		for _, tc := range testCases {
			cfg := ServiceConfig{
				ClusterID: tc.clusterID,
				NodeID:    tc.nodeID,
			}
			assert.Equal(t, tc.expectedBase, cfg.SubjectBase())
		}
	})

	t.Run("service name format", func(t *testing.T) {
		testCases := []struct {
			clusterID    string
			nodeID       string
			expectedName string
		}{
			{"my-cluster", "node-1", "convex-my-cluster-node-1"},
			{"prod", "server-a", "convex-prod-server-a"},
			{"test-env", "node1", "convex-test-env-node1"},
		}

		for _, tc := range testCases {
			cfg := ServiceConfig{
				ClusterID: tc.clusterID,
				NodeID:    tc.nodeID,
			}
			assert.Equal(t, tc.expectedName, cfg.ServiceName())
		}
	})
}

func TestNodeStatus(t *testing.T) {
	t.Run("json marshaling", func(t *testing.T) {
		status := NodeStatus{
			NodeID:         "node-1",
			ClusterID:      "my-cluster",
			Role:           "PRIMARY",
			Leader:         "node-1",
			BackendStatus:  "running",
			ReplicationLag: 100,
			WALSequence:    12345,
			UptimeMs:       5000,
			Timestamp:      1234567890000,
		}

		data, err := json.Marshal(status)
		require.NoError(t, err)

		var decoded NodeStatus
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, status, decoded)
	})
}
