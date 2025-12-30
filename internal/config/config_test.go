package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromFile(t *testing.T) {
	t.Run("loads valid config file", func(t *testing.T) {
		// Create temp config file
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "cluster.json")
		configJSON := `{
			"clusterId": "test-cluster",
			"nodeId": "node-1",
			"nats": {
				"servers": ["nats://localhost:4222"]
			},
			"vip": {
				"address": "192.168.1.100",
				"netmask": 24,
				"interface": "eth0"
			},
			"election": {
				"leaderTtlMs": 10000,
				"heartbeatIntervalMs": 3000
			},
			"wal": {
				"streamRetention": "24h",
				"snapshotIntervalMs": 3600000,
				"replicaPath": "/var/lib/convex/replica"
			},
			"backend": {
				"serviceName": "convex-backend",
				"healthEndpoint": "http://localhost:3210/version",
				"dataPath": "/var/lib/convex/data"
			}
		}`
		err := os.WriteFile(configPath, []byte(configJSON), 0644)
		require.NoError(t, err)

		// Load config
		cfg, err := LoadFromFile(configPath)
		require.NoError(t, err)

		// Verify values
		assert.Equal(t, "test-cluster", cfg.ClusterID)
		assert.Equal(t, "node-1", cfg.NodeID)
		assert.Equal(t, []string{"nats://localhost:4222"}, cfg.NATS.Servers)
		assert.Equal(t, "192.168.1.100", cfg.VIP.Address)
		assert.Equal(t, 24, cfg.VIP.Netmask)
		assert.Equal(t, "eth0", cfg.VIP.Interface)
		assert.Equal(t, 10*time.Second, cfg.Election.LeaderTTL)
		assert.Equal(t, 3*time.Second, cfg.Election.HeartbeatInterval)
		assert.Equal(t, 24*time.Hour, cfg.WAL.StreamRetention)
		assert.Equal(t, time.Hour, cfg.WAL.SnapshotInterval)
		assert.Equal(t, "/var/lib/convex/replica", cfg.WAL.ReplicaPath)
		assert.Equal(t, "convex-backend", cfg.Backend.ServiceName)
		assert.Equal(t, "http://localhost:3210/version", cfg.Backend.HealthEndpoint)
		assert.Equal(t, "/var/lib/convex/data", cfg.Backend.DataPath)
	})

	t.Run("returns error for missing file", func(t *testing.T) {
		_, err := LoadFromFile("/nonexistent/path/config.json")
		assert.Error(t, err)
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "invalid.json")
		err := os.WriteFile(configPath, []byte("not valid json"), 0644)
		require.NoError(t, err)

		_, err = LoadFromFile(configPath)
		assert.Error(t, err)
	})
}

func TestConfigValidation(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		cfg := &Config{}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "clusterId")
	})

	t.Run("validates clusterId is required", func(t *testing.T) {
		cfg := &Config{
			NodeID: "node-1",
			NATS:   NATSConfig{Servers: []string{"nats://localhost:4222"}},
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "clusterId")
	})

	t.Run("validates nodeId is required", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nodeId")
	})

	t.Run("validates NATS servers are required", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nats.servers")
	})

	t.Run("validates VIP address format", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
			VIP:       VIPConfig{Address: "invalid-ip", Netmask: 24, Interface: "eth0"},
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "vip.address")
	})

	t.Run("validates VIP netmask range", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
			VIP:       VIPConfig{Address: "192.168.1.100", Netmask: 33, Interface: "eth0"},
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "vip.netmask")
	})

	t.Run("passes with valid minimal config", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("passes with valid full config", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
			VIP:       VIPConfig{Address: "192.168.1.100", Netmask: 24, Interface: "eth0"},
			Election:  ElectionConfig{LeaderTTL: 10 * time.Second, HeartbeatInterval: 3 * time.Second},
			WAL:       WALConfig{StreamRetention: 24 * time.Hour, SnapshotInterval: time.Hour, ReplicaPath: "/tmp/replica"},
			Backend:   BackendConfig{ServiceName: "convex-backend", HealthEndpoint: "http://localhost:3210/version", DataPath: "/tmp/data"},
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})
}

func TestConfigDefaults(t *testing.T) {
	t.Run("applies default election settings", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
		}
		cfg.ApplyDefaults()

		assert.Equal(t, DefaultLeaderTTL, cfg.Election.LeaderTTL)
		assert.Equal(t, DefaultHeartbeatInterval, cfg.Election.HeartbeatInterval)
	})

	t.Run("applies default WAL settings", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
		}
		cfg.ApplyDefaults()

		assert.Equal(t, DefaultStreamRetention, cfg.WAL.StreamRetention)
		assert.Equal(t, DefaultSnapshotInterval, cfg.WAL.SnapshotInterval)
		assert.Equal(t, DefaultReplicaPath, cfg.WAL.ReplicaPath)
	})

	t.Run("applies default backend settings", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
		}
		cfg.ApplyDefaults()

		assert.Equal(t, DefaultServiceName, cfg.Backend.ServiceName)
		assert.Equal(t, DefaultHealthEndpoint, cfg.Backend.HealthEndpoint)
		assert.Equal(t, DefaultDataPath, cfg.Backend.DataPath)
	})

	t.Run("does not override existing values", func(t *testing.T) {
		cfg := &Config{
			ClusterID: "cluster-1",
			NodeID:    "node-1",
			NATS:      NATSConfig{Servers: []string{"nats://localhost:4222"}},
			Election:  ElectionConfig{LeaderTTL: 20 * time.Second},
		}
		cfg.ApplyDefaults()

		assert.Equal(t, 20*time.Second, cfg.Election.LeaderTTL)
		assert.Equal(t, DefaultHeartbeatInterval, cfg.Election.HeartbeatInterval)
	})
}

func TestVIPConfigCIDR(t *testing.T) {
	t.Run("returns correct CIDR notation", func(t *testing.T) {
		vip := VIPConfig{Address: "192.168.1.100", Netmask: 24, Interface: "eth0"}
		assert.Equal(t, "192.168.1.100/24", vip.CIDR())
	})
}
