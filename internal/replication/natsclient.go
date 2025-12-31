package replication

import (
	"log/slog"
	"strings"

	litestreamnats "github.com/benbjohnson/litestream/nats"
)

// ReplicaClientConfig contains configuration for creating a NATS replica client.
type ReplicaClientConfig struct {
	// NATSURLs contains NATS server URLs for automatic failover.
	// Multiple URLs are joined with commas for the litestream client.
	NATSURLs []string

	// NATSCredentials is an optional path to a NATS credentials file.
	NATSCredentials string

	// BucketName is the NATS Object Store bucket name.
	BucketName string

	// Path is an optional logical path prefix within the bucket.
	Path string

	// Logger for connection events.
	Logger *slog.Logger
}

// NewReplicaClient creates a new litestream NATS replica client with consistent
// configuration for resilient connections.
//
// The litestream NATS client now includes full resilience features:
// - Automatic reconnection on disconnect (unlimited retries)
// - Failover to other servers when multiple URLs are provided
// - Retry on initial connection failures
// - Cluster discovery via gossip protocol
// - Connection lifecycle logging (disconnect, reconnect, discovered servers, errors)
//
// This factory ensures all replication components use the same configuration
// pattern and provides logging for better observability.
func NewReplicaClient(cfg ReplicaClientConfig) *litestreamnats.ReplicaClient {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	client := litestreamnats.NewReplicaClient()

	// Configure with all NATS URLs for automatic failover.
	// The nats.go client will try each URL in order and reconnect
	// to any available server on disconnect.
	if len(cfg.NATSURLs) > 0 {
		client.URL = strings.Join(cfg.NATSURLs, ",")
	}

	client.BucketName = cfg.BucketName

	if cfg.Path != "" {
		client.Path = cfg.Path
	}

	if cfg.NATSCredentials != "" {
		client.Creds = cfg.NATSCredentials
	}

	// Inject logger for connection lifecycle events.
	// The litestream client will use this for disconnect, reconnect,
	// discovered servers, and error logging.
	client.Logger = logger.With("component", "litestream-nats")

	logger.Debug("created litestream NATS replica client",
		"urls", cfg.NATSURLs,
		"bucket", cfg.BucketName,
		"path", cfg.Path,
		"has_credentials", cfg.NATSCredentials != "",
	)

	return client
}

// BucketNameForCluster returns the standard NATS Object Store bucket name
// for WAL replication for a given cluster ID.
// Format: convex-<cluster_id>-wal
func BucketNameForCluster(clusterID string) string {
	return "convex-" + clusterID + "-wal"
}
