// Package config provides configuration loading and validation for the cluster manager.
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

// Default configuration values
const (
	DefaultLeaderTTL          = 10 * time.Second
	DefaultHeartbeatInterval  = 3 * time.Second
	DefaultStreamRetention    = 24 * time.Hour
	DefaultSnapshotInterval   = time.Hour
	DefaultReplicaPath        = "/var/lib/convex/replica"
	DefaultServiceName        = "convex-backend"
	DefaultHealthEndpoint     = "http://localhost:3210/version"
	DefaultDataPath           = "/var/lib/convex/data"
)

// Config represents the cluster manager configuration.
type Config struct {
	ClusterID string         `json:"clusterId"`
	NodeID    string         `json:"nodeId"`
	NATS      NATSConfig     `json:"nats"`
	VIP       VIPConfig      `json:"vip"`
	Election  ElectionConfig `json:"election"`
	WAL       WALConfig      `json:"wal"`
	Backend   BackendConfig  `json:"backend"`
}

// NATSConfig contains NATS connection settings.
type NATSConfig struct {
	Servers     []string `json:"servers"`
	Credentials string   `json:"credentials,omitempty"`
}

// VIPConfig contains Virtual IP settings.
type VIPConfig struct {
	Address   string `json:"address"`
	Netmask   int    `json:"netmask"`
	Interface string `json:"interface"`
}

// CIDR returns the VIP address in CIDR notation.
func (v VIPConfig) CIDR() string {
	return fmt.Sprintf("%s/%d", v.Address, v.Netmask)
}

// ElectionConfig contains leader election settings.
type ElectionConfig struct {
	LeaderTTL         time.Duration `json:"-"`
	HeartbeatInterval time.Duration `json:"-"`
}

// WALConfig contains WAL replication settings.
type WALConfig struct {
	StreamRetention  time.Duration `json:"-"`
	SnapshotInterval time.Duration `json:"-"`
	ReplicaPath      string        `json:"replicaPath"`
}

// BackendConfig contains backend service settings.
type BackendConfig struct {
	ServiceName    string `json:"serviceName"`
	HealthEndpoint string `json:"healthEndpoint"`
	DataPath       string `json:"dataPath"`
}

// rawConfig is used for JSON unmarshaling with millisecond durations.
type rawConfig struct {
	ClusterID string `json:"clusterId"`
	NodeID    string `json:"nodeId"`
	NATS      struct {
		Servers     []string `json:"servers"`
		Credentials string   `json:"credentials,omitempty"`
	} `json:"nats"`
	VIP struct {
		Address   string `json:"address"`
		Netmask   int    `json:"netmask"`
		Interface string `json:"interface"`
	} `json:"vip"`
	Election struct {
		LeaderTTLMs         int64 `json:"leaderTtlMs"`
		HeartbeatIntervalMs int64 `json:"heartbeatIntervalMs"`
	} `json:"election"`
	WAL struct {
		StreamRetention    string `json:"streamRetention"`
		SnapshotIntervalMs int64  `json:"snapshotIntervalMs"`
		ReplicaPath        string `json:"replicaPath"`
	} `json:"wal"`
	Backend struct {
		ServiceName    string `json:"serviceName"`
		HealthEndpoint string `json:"healthEndpoint"`
		DataPath       string `json:"dataPath"`
	} `json:"backend"`
}

// LoadFromFile loads configuration from a JSON file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var raw rawConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg := &Config{
		ClusterID: raw.ClusterID,
		NodeID:    raw.NodeID,
		NATS: NATSConfig{
			Servers:     raw.NATS.Servers,
			Credentials: raw.NATS.Credentials,
		},
		VIP: VIPConfig{
			Address:   raw.VIP.Address,
			Netmask:   raw.VIP.Netmask,
			Interface: raw.VIP.Interface,
		},
		Election: ElectionConfig{
			LeaderTTL:         time.Duration(raw.Election.LeaderTTLMs) * time.Millisecond,
			HeartbeatInterval: time.Duration(raw.Election.HeartbeatIntervalMs) * time.Millisecond,
		},
		WAL: WALConfig{
			SnapshotInterval: time.Duration(raw.WAL.SnapshotIntervalMs) * time.Millisecond,
			ReplicaPath:      raw.WAL.ReplicaPath,
		},
		Backend: BackendConfig{
			ServiceName:    raw.Backend.ServiceName,
			HealthEndpoint: raw.Backend.HealthEndpoint,
			DataPath:       raw.Backend.DataPath,
		},
	}

	// Parse stream retention as duration string (e.g., "24h")
	if raw.WAL.StreamRetention != "" {
		retention, err := time.ParseDuration(raw.WAL.StreamRetention)
		if err != nil {
			return nil, fmt.Errorf("failed to parse wal.streamRetention: %w", err)
		}
		cfg.WAL.StreamRetention = retention
	}

	return cfg, nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("clusterId is required")
	}
	if c.NodeID == "" {
		return fmt.Errorf("nodeId is required")
	}
	if len(c.NATS.Servers) == 0 {
		return fmt.Errorf("nats.servers is required")
	}

	// Validate VIP if configured
	if c.VIP.Address != "" {
		ip := net.ParseIP(c.VIP.Address)
		if ip == nil {
			return fmt.Errorf("vip.address is not a valid IP address")
		}
		if c.VIP.Netmask < 0 || c.VIP.Netmask > 32 {
			return fmt.Errorf("vip.netmask must be between 0 and 32")
		}
		if c.VIP.Interface == "" {
			return fmt.Errorf("vip.interface is required when vip.address is set")
		}
	}

	return nil
}

// ApplyDefaults applies default values to unset configuration fields.
func (c *Config) ApplyDefaults() {
	// Election defaults
	if c.Election.LeaderTTL == 0 {
		c.Election.LeaderTTL = DefaultLeaderTTL
	}
	if c.Election.HeartbeatInterval == 0 {
		c.Election.HeartbeatInterval = DefaultHeartbeatInterval
	}

	// WAL defaults
	if c.WAL.StreamRetention == 0 {
		c.WAL.StreamRetention = DefaultStreamRetention
	}
	if c.WAL.SnapshotInterval == 0 {
		c.WAL.SnapshotInterval = DefaultSnapshotInterval
	}
	if c.WAL.ReplicaPath == "" {
		c.WAL.ReplicaPath = DefaultReplicaPath
	}

	// Backend defaults
	if c.Backend.ServiceName == "" {
		c.Backend.ServiceName = DefaultServiceName
	}
	if c.Backend.HealthEndpoint == "" {
		c.Backend.HealthEndpoint = DefaultHealthEndpoint
	}
	if c.Backend.DataPath == "" {
		c.Backend.DataPath = DefaultDataPath
	}
}
