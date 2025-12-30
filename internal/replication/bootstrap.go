package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/benbjohnson/litestream"
	litestreamnats "github.com/benbjohnson/litestream/nats"
)

// ErrNoSnapshots is returned when no snapshots are available for bootstrap.
var ErrNoSnapshots = errors.New("no snapshots available")

// BootstrapConfig configures the bootstrap process for new nodes.
type BootstrapConfig struct {
	// ClusterID is used to determine the NATS Object Store bucket name.
	ClusterID string

	// NATSURLs contains NATS server URLs. Only the first is used.
	NATSURLs []string

	// NATSCredentials is an optional path to a NATS credentials file.
	NATSCredentials string

	// OutputPath is the path where the database will be restored.
	OutputPath string
}

// Validate validates the bootstrap configuration.
func (c BootstrapConfig) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("clusterID is required")
	}
	if len(c.NATSURLs) == 0 {
		return fmt.Errorf("at least one NATS URL is required")
	}
	if c.OutputPath == "" {
		return fmt.Errorf("outputPath is required")
	}
	return nil
}

// BucketName returns the NATS Object Store bucket name for the cluster.
func (c BootstrapConfig) BucketName() string {
	return fmt.Sprintf("convex-%s-wal", c.ClusterID)
}

// Bootstrap downloads and restores the latest snapshot from the cluster's
// NATS Object Store bucket. This is used when a new node joins the cluster
// to bootstrap its initial data.
//
// If no snapshots are available (e.g., new cluster), ErrNoSnapshots is returned.
// The caller should handle this case appropriately.
func Bootstrap(ctx context.Context, cfg BootstrapConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	logger := slog.Default().With(
		"component", "replication-bootstrap",
		"cluster", cfg.ClusterID,
	)

	logger.Info("starting bootstrap",
		"bucket", cfg.BucketName(),
		"nats", cfg.NATSURLs[0],
		"outputPath", cfg.OutputPath,
	)

	// Ensure output directory exists.
	outputDir := filepath.Dir(cfg.OutputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	// Create NATS replica client.
	client := litestreamnats.NewReplicaClient()
	client.URL = cfg.NATSURLs[0]
	client.BucketName = cfg.BucketName()
	if cfg.NATSCredentials != "" {
		client.Creds = cfg.NATSCredentials
	}

	// Create replica without an attached DB (for restore-only operation).
	replica := litestream.NewReplica(nil)
	replica.Client = client

	// Check if there are any snapshots available.
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = cfg.OutputPath

	updatedAt, err := replica.CalcRestoreTarget(ctx, opt)
	if err != nil {
		_ = client.Close()
		// Check if this is a "no snapshots" situation.
		if isNoSnapshotsError(err) {
			logger.Info("no snapshots available for bootstrap")
			return ErrNoSnapshots
		}
		return fmt.Errorf("calc restore target: %w", err)
	}

	if updatedAt.IsZero() {
		_ = client.Close()
		logger.Info("no snapshots available for bootstrap")
		return ErrNoSnapshots
	}

	// Restore the snapshot to the output path.
	if err := replica.Restore(ctx, opt); err != nil {
		_ = client.Close()
		return fmt.Errorf("restore from replica: %w", err)
	}

	if err := client.Close(); err != nil {
		logger.Warn("failed to close NATS client", "error", err)
	}

	logger.Info("bootstrap completed successfully",
		"updatedAt", updatedAt,
		"outputPath", cfg.OutputPath,
	)

	return nil
}

// isNoSnapshotsError checks if the error indicates no snapshots are available.
func isNoSnapshotsError(err error) bool {
	if err == nil {
		return false
	}
	// Litestream returns various errors when no snapshots exist.
	// Check for common patterns.
	errStr := err.Error()
	return errors.Is(err, litestream.ErrNoSnapshots) ||
		containsAny(errStr, "no snapshots", "no generations", "bucket not found", "key not found")
}

// containsAny returns true if s contains any of the substrings.
func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
