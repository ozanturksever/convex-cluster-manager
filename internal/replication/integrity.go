package replication

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/benbjohnson/litestream"
	litestreamnats "github.com/benbjohnson/litestream/nats"
	"github.com/superfly/ltx"
)

// ltxTXID converts uint64 to ltx.TXID
func ltxTXID(id uint64) ltx.TXID {
	return ltx.TXID(id)
}

// IntegrityChecker provides data integrity verification for SQLite databases
// replicated via Litestream to NATS JetStream.
type IntegrityChecker struct {
	cfg    Config
	logger *slog.Logger
}

// IntegrityReport contains the results of a data integrity check.
type IntegrityReport struct {
	// DatabasePath is the path to the database that was checked.
	DatabasePath string

	// DatabaseExists indicates whether the database file exists.
	DatabaseExists bool

	// DatabaseValid indicates whether the database passes SQLite integrity check.
	DatabaseValid bool

	// WALMode indicates whether the database is in WAL mode.
	WALMode bool

	// PageCount is the number of pages in the database.
	PageCount int64

	// ReplicaReachable indicates whether the NATS replica is reachable.
	ReplicaReachable bool

	// ReplicaLTXCount is the number of LTX files in the replica.
	ReplicaLTXCount int

	// ReplicaMaxTXID is the maximum transaction ID in the replica.
	ReplicaMaxTXID uint64

	// Errors contains any errors encountered during the check.
	Errors []error
}

// IsHealthy returns true if all critical checks pass.
func (r *IntegrityReport) IsHealthy() bool {
	if !r.DatabaseExists {
		return true // No database yet is acceptable
	}
	return r.DatabaseValid && r.WALMode && len(r.Errors) == 0
}

// NewIntegrityChecker creates a new integrity checker.
// Unlike Primary/Passive, the integrity checker allows empty NATSURLs
// for local-only database checks.
func NewIntegrityChecker(cfg Config) (*IntegrityChecker, error) {
	// Use relaxed validation - NATS URLs are optional for integrity checking
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("clusterID is required")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("nodeID is required")
	}
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("dbPath is required")
	}

	return &IntegrityChecker{
		cfg: cfg,
		logger: slog.Default().With(
			"component", "integrity-checker",
			"cluster", cfg.ClusterID,
			"node", cfg.NodeID,
		),
	}, nil
}

// Check performs a comprehensive data integrity check.
func (ic *IntegrityChecker) Check(ctx context.Context) (*IntegrityReport, error) {
	report := &IntegrityReport{
		DatabasePath: ic.cfg.DBPath,
	}

	// Check if database exists
	if _, err := os.Stat(ic.cfg.DBPath); os.IsNotExist(err) {
		report.DatabaseExists = false
		ic.logger.Debug("database does not exist", "path", ic.cfg.DBPath)
		return report, nil
	}
	report.DatabaseExists = true

	// Check database integrity
	if err := ic.checkDatabaseIntegrity(ctx, report); err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("database integrity: %w", err))
	}

	// Check replica reachability and state
	if err := ic.checkReplica(ctx, report); err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("replica check: %w", err))
	}

	return report, nil
}

// checkDatabaseIntegrity verifies the local SQLite database.
func (ic *IntegrityChecker) checkDatabaseIntegrity(ctx context.Context, report *IntegrityReport) error {
	db, err := sql.Open("sqlite", ic.cfg.DBPath+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Check WAL mode
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		return fmt.Errorf("query journal mode: %w", err)
	}
	report.WALMode = journalMode == "wal"

	// Check page count
	if err := db.QueryRowContext(ctx, "PRAGMA page_count;").Scan(&report.PageCount); err != nil {
		return fmt.Errorf("query page count: %w", err)
	}

	// Run SQLite integrity check
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check;").Scan(&result); err != nil {
		return fmt.Errorf("integrity check: %w", err)
	}
	report.DatabaseValid = result == "ok"

	if !report.DatabaseValid {
		ic.logger.Error("database integrity check failed", "result", result)
	}

	return nil
}

// checkReplica verifies the NATS replica state.
func (ic *IntegrityChecker) checkReplica(ctx context.Context, report *IntegrityReport) error {
	if len(ic.cfg.NATSURLs) == 0 {
		return nil // No replica configured
	}

	client := litestreamnats.NewReplicaClient()
	client.URL = ic.cfg.NATSURLs[0]
	client.BucketName = fmt.Sprintf("convex-%s-wal", ic.cfg.ClusterID)
	client.Path = ic.cfg.ReplicaPath
	if ic.cfg.NATSCredentials != "" {
		client.Creds = ic.cfg.NATSCredentials
	}
	defer client.Close()

	if err := client.Init(ctx); err != nil {
		report.ReplicaReachable = false
		return fmt.Errorf("init replica client: %w", err)
	}
	report.ReplicaReachable = true

	// Count LTX files and find max TXID
	itr, err := client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return fmt.Errorf("list LTX files: %w", err)
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Item()
		report.ReplicaLTXCount++
		if uint64(info.MaxTXID) > report.ReplicaMaxTXID {
			report.ReplicaMaxTXID = uint64(info.MaxTXID)
		}
	}

	return nil
}

// ValidateLTXFile validates a single LTX file from the replica.
func (ic *IntegrityChecker) ValidateLTXFile(ctx context.Context, level int, minTXID, maxTXID uint64) error {
	if len(ic.cfg.NATSURLs) == 0 {
		return fmt.Errorf("no NATS URL configured")
	}

	client := litestreamnats.NewReplicaClient()
	client.URL = ic.cfg.NATSURLs[0]
	client.BucketName = fmt.Sprintf("convex-%s-wal", ic.cfg.ClusterID)
	client.Path = ic.cfg.ReplicaPath
	if ic.cfg.NATSCredentials != "" {
		client.Creds = ic.cfg.NATSCredentials
	}
	defer client.Close()

	if err := client.Init(ctx); err != nil {
		return fmt.Errorf("init replica client: %w", err)
	}

	// Open the LTX file using ltx.TXID types
	rd, err := client.OpenLTXFile(ctx, level, ltxTXID(minTXID), ltxTXID(maxTXID), 0, 0)
	if err != nil {
		return fmt.Errorf("open LTX file: %w", err)
	}
	defer rd.Close()

	// Validate using litestream's built-in validation
	if err := litestream.ValidateLTX(rd); err != nil {
		return fmt.Errorf("LTX validation failed: %w", err)
	}

	ic.logger.Debug("LTX file validated",
		"level", level,
		"minTXID", minTXID,
		"maxTXID", maxTXID,
	)

	return nil
}

// ValidateAllLTXFiles validates all LTX files in the replica.
// This is expensive and should only be used for scrubbing/verification jobs.
func (ic *IntegrityChecker) ValidateAllLTXFiles(ctx context.Context) (validated int, errors []error) {
	if len(ic.cfg.NATSURLs) == 0 {
		return 0, []error{fmt.Errorf("no NATS URL configured")}
	}

	client := litestreamnats.NewReplicaClient()
	client.URL = ic.cfg.NATSURLs[0]
	client.BucketName = fmt.Sprintf("convex-%s-wal", ic.cfg.ClusterID)
	client.Path = ic.cfg.ReplicaPath
	if ic.cfg.NATSCredentials != "" {
		client.Creds = ic.cfg.NATSCredentials
	}
	defer client.Close()

	if err := client.Init(ctx); err != nil {
		return 0, []error{fmt.Errorf("init replica client: %w", err)}
	}

	// Validate L0 files
	for level := 0; level <= litestream.SnapshotLevel; level++ {
		itr, err := client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			errors = append(errors, fmt.Errorf("list level %d: %w", level, err))
			continue
		}

		for itr.Next() {
			info := itr.Item()

			rd, err := client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
			if err != nil {
				errors = append(errors, fmt.Errorf("open L%d %s-%s: %w",
					info.Level, info.MinTXID, info.MaxTXID, err))
				continue
			}

			if err := litestream.ValidateLTX(rd); err != nil {
				errors = append(errors, fmt.Errorf("validate L%d %s-%s: %w",
					info.Level, info.MinTXID, info.MaxTXID, err))
				rd.Close()
				continue
			}

			rd.Close()
			validated++
		}

		itr.Close()
	}

	return validated, errors
}

// CompareWithReplica compares the local database state with the replica.
// Returns the local TXID, replica TXID, and whether they match.
func (ic *IntegrityChecker) CompareWithReplica(ctx context.Context) (localTXID, replicaTXID uint64, match bool, err error) {
	// Get local database state
	if _, statErr := os.Stat(ic.cfg.DBPath); os.IsNotExist(statErr) {
		// No local database, can't compare
		return 0, 0, false, nil
	}

	// Use litestream to get local position
	db := litestream.NewDB(ic.cfg.DBPath)
	if err := db.Open(); err != nil {
		return 0, 0, false, fmt.Errorf("open litestream db: %w", err)
	}
	defer db.Close(ctx)

	pos, err := db.Pos()
	if err != nil {
		return 0, 0, false, fmt.Errorf("get db position: %w", err)
	}
	localTXID = uint64(pos.TXID)

	// Get replica state
	if len(ic.cfg.NATSURLs) == 0 {
		return localTXID, 0, false, nil
	}

	client := litestreamnats.NewReplicaClient()
	client.URL = ic.cfg.NATSURLs[0]
	client.BucketName = fmt.Sprintf("convex-%s-wal", ic.cfg.ClusterID)
	client.Path = ic.cfg.ReplicaPath
	if ic.cfg.NATSCredentials != "" {
		client.Creds = ic.cfg.NATSCredentials
	}
	defer client.Close()

	if err := client.Init(ctx); err != nil {
		return localTXID, 0, false, fmt.Errorf("init replica client: %w", err)
	}

	replica := litestream.NewReplica(nil)
	replica.Client = client

	info, err := replica.MaxLTXFileInfo(ctx, 0)
	if err != nil {
		return localTXID, 0, false, fmt.Errorf("get replica max TXID: %w", err)
	}
	replicaTXID = uint64(info.MaxTXID)

	match = localTXID == replicaTXID

	ic.logger.Debug("compared with replica",
		"localTXID", localTXID,
		"replicaTXID", replicaTXID,
		"match", match,
	)

	return localTXID, replicaTXID, match, nil
}

// ltxReader wraps io.ReadCloser to satisfy the Reader interface needed by ValidateLTX.
type ltxReader struct {
	io.ReadCloser
}

func (r *ltxReader) Read(p []byte) (n int, err error) {
	return r.ReadCloser.Read(p)
}
