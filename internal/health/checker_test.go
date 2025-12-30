package health

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

func TestHealthChecker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start NATS container
	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	t.Run("responds to health check requests", func(t *testing.T) {
		cfg := Config{
			ClusterID: "test-cluster",
			NodeID:    "node-1",
			NATSURLs:  []string{natsContainer.URL},
		}

		checker, err := NewChecker(cfg)
		require.NoError(t, err)

		// Set some state
		checker.SetRole("PRIMARY")
		checker.SetLeader("node-1")
		checker.SetBackendStatus("running")

		err = checker.Start(ctx)
		require.NoError(t, err)
		defer checker.Stop()

		// Connect a client to send health check request
		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		// Send health check request
		subject := "convex.health.node-1"
		msg, err := nc.Request(subject, nil, 5*time.Second)
		require.NoError(t, err)

		// Parse response
		var response HealthResponse
		err = json.Unmarshal(msg.Data, &response)
		require.NoError(t, err)

		assert.Equal(t, "node-1", response.NodeID)
		assert.Equal(t, "PRIMARY", response.Role)
		assert.Equal(t, "node-1", response.Leader)
		assert.Equal(t, "running", response.BackendStatus)
		assert.NotZero(t, response.Timestamp)
	})

	t.Run("updates state dynamically", func(t *testing.T) {
		cfg := Config{
			ClusterID: "test-cluster",
			NodeID:    "node-2",
			NATSURLs:  []string{natsContainer.URL},
		}

		checker, err := NewChecker(cfg)
		require.NoError(t, err)

		checker.SetRole("PASSIVE")
		checker.SetLeader("node-1")

		err = checker.Start(ctx)
		require.NoError(t, err)
		defer checker.Stop()

		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		// First request - PASSIVE
		msg, err := nc.Request("convex.health.node-2", nil, 5*time.Second)
		require.NoError(t, err)

		var response HealthResponse
		err = json.Unmarshal(msg.Data, &response)
		require.NoError(t, err)
		assert.Equal(t, "PASSIVE", response.Role)

		// Update state
		checker.SetRole("PRIMARY")
		checker.SetLeader("node-2")

		// Second request - should reflect new state
		msg, err = nc.Request("convex.health.node-2", nil, 5*time.Second)
		require.NoError(t, err)

		err = json.Unmarshal(msg.Data, &response)
		require.NoError(t, err)
		assert.Equal(t, "PRIMARY", response.Role)
		assert.Equal(t, "node-2", response.Leader)
	})

	t.Run("tracks uptime", func(t *testing.T) {
		cfg := Config{
			ClusterID: "test-cluster",
			NodeID:    "node-3",
			NATSURLs:  []string{natsContainer.URL},
		}

		checker, err := NewChecker(cfg)
		require.NoError(t, err)

		err = checker.Start(ctx)
		require.NoError(t, err)
		defer checker.Stop()

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		msg, err := nc.Request("convex.health.node-3", nil, 5*time.Second)
		require.NoError(t, err)

		var response HealthResponse
		err = json.Unmarshal(msg.Data, &response)
		require.NoError(t, err)

		assert.Greater(t, response.UptimeMs, int64(0))
	})

	t.Run("tracks replication lag", func(t *testing.T) {
		cfg := Config{
			ClusterID: "test-cluster",
			NodeID:    "node-4",
			NATSURLs:  []string{natsContainer.URL},
		}

		checker, err := NewChecker(cfg)
		require.NoError(t, err)

		checker.SetReplicationLag(5)
		checker.SetWALSequence(12345)

		err = checker.Start(ctx)
		require.NoError(t, err)
		defer checker.Stop()

		nc, err := nats.Connect(natsContainer.URL)
		require.NoError(t, err)
		defer nc.Close()

		msg, err := nc.Request("convex.health.node-4", nil, 5*time.Second)
		require.NoError(t, err)

		var response HealthResponse
		err = json.Unmarshal(msg.Data, &response)
		require.NoError(t, err)

		assert.Equal(t, int64(5), response.ReplicationLag)
		assert.Equal(t, uint64(12345), response.WALSequence)
	})

	t.Run("can query other nodes", func(t *testing.T) {
		// Start two checkers
		cfg1 := Config{
			ClusterID: "test-cluster",
			NodeID:    "node-a",
			NATSURLs:  []string{natsContainer.URL},
		}
		checker1, err := NewChecker(cfg1)
		require.NoError(t, err)
		checker1.SetRole("PRIMARY")
		err = checker1.Start(ctx)
		require.NoError(t, err)
		defer checker1.Stop()

		cfg2 := Config{
			ClusterID: "test-cluster",
			NodeID:    "node-b",
			NATSURLs:  []string{natsContainer.URL},
		}
		checker2, err := NewChecker(cfg2)
		require.NoError(t, err)
		checker2.SetRole("PASSIVE")
		err = checker2.Start(ctx)
		require.NoError(t, err)
		defer checker2.Stop()

		// Query node-a from node-b's perspective
		response, err := checker2.QueryNode(ctx, "node-a", 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "node-a", response.NodeID)
		assert.Equal(t, "PRIMARY", response.Role)

		// Query node-b from node-a's perspective
		response, err = checker1.QueryNode(ctx, "node-b", 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "node-b", response.NodeID)
		assert.Equal(t, "PASSIVE", response.Role)
	})
}

func TestHealthCheckerConfig(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		cfg := Config{}
		_, err := NewChecker(cfg)
		assert.Error(t, err)
	})

	t.Run("validates cluster ID required", func(t *testing.T) {
		cfg := Config{
			NodeID:   "node-1",
			NATSURLs: []string{"nats://localhost:4222"},
		}
		_, err := NewChecker(cfg)
		assert.Error(t, err)
	})

	t.Run("validates node ID required", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		_, err := NewChecker(cfg)
		assert.Error(t, err)
	})

	t.Run("validates NATS URLs required", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
		}
		_, err := NewChecker(cfg)
		assert.Error(t, err)
	})

	t.Run("passes with valid config", func(t *testing.T) {
		cfg := Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATSURLs:  []string{"nats://localhost:4222"},
		}
		checker, err := NewChecker(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, checker)
	})
}

func TestHealthResponse(t *testing.T) {
	t.Run("JSON serialization", func(t *testing.T) {
		response := HealthResponse{
			NodeID:         "node-1",
			Role:           "PRIMARY",
			Leader:         "node-1",
			ReplicationLag: 0,
			WALSequence:    12345,
			BackendStatus:  "running",
			UptimeMs:       86400000,
			Timestamp:      time.Now().UnixMilli(),
		}

		data, err := json.Marshal(response)
		require.NoError(t, err)

		var parsed HealthResponse
		err = json.Unmarshal(data, &parsed)
		require.NoError(t, err)

		assert.Equal(t, response.NodeID, parsed.NodeID)
		assert.Equal(t, response.Role, parsed.Role)
		assert.Equal(t, response.WALSequence, parsed.WALSequence)
	})
}
