package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ozanturksever/convex-cluster-manager/internal/natsutil"
)

// Election errors
var (
	ErrElectionClosed = errors.New("election closed")
)

// ElectionConfig contains configuration for leader election.
type ElectionConfig struct {
	ClusterID         string
	NodeID            string
	NATSURLs          []string
	NATSCredentials   string
	LeaderTTL         time.Duration
	HeartbeatInterval time.Duration
}

// BucketName returns the auth-compatible KV bucket name for election.
// Format: convex-<cluster_id>-election
// This allows NATS auth rules like: $KV.convex-mycluster-election.>
func (c ElectionConfig) BucketName() string {
	return fmt.Sprintf("convex-%s-election", c.ClusterID)
}

// Election manages leader election using NATS KV store with watchers.
// It uses KV TTL for lease-based leadership and KV watchers for
// real-time leader change detection instead of polling.
type Election struct {
	cfg    ElectionConfig
	nc     *nats.Conn
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	logger *slog.Logger

	mu            sync.RWMutex
	isLeader      bool
	currentLeader string
	revision      uint64

	// renewFailures tracks consecutive renewal failures for graceful degradation
	renewFailures int

	// Channels for coordination
	leaderCh chan struct{} // Notifies when this node becomes leader
	stopCh   chan struct{} // Signals shutdown

	// Watcher for real-time leader changes
	watcher jetstream.KeyWatcher

	wg sync.WaitGroup
}

// NewElection creates a new Election instance.
func NewElection(cfg ElectionConfig) (*Election, error) {
	if cfg.ClusterID == "" {
		return nil, errors.New("clusterID is required")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("nodeID is required")
	}
	if len(cfg.NATSURLs) == 0 {
		return nil, errors.New("at least one NATS URL is required")
	}

	// Apply defaults
	if cfg.LeaderTTL == 0 {
		cfg.LeaderTTL = 10 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 3 * time.Second
	}

	return &Election{
		cfg:      cfg,
		logger:   slog.Default().With("component", "election", "node", cfg.NodeID, "cluster", cfg.ClusterID),
		leaderCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}, nil
}

// Start begins the election process.
// It connects to NATS, creates/gets the KV bucket, starts the KV watcher,
// and attempts to acquire leadership.
func (e *Election) Start(ctx context.Context) error {
	// Reset stop channel for restart capability
	e.stopCh = make(chan struct{})

	// Connect to NATS with automatic failover and cluster discovery
	nc, err := natsutil.Connect(natsutil.ConnectOptions{
		URLs:        e.cfg.NATSURLs,
		Credentials: e.cfg.NATSCredentials,
		Logger:      e.logger,
	})
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	e.nc = nc

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("create JetStream context: %w", err)
	}
	e.js = js

	// Create or get KV bucket for leader election
	// Using auth-compatible bucket name: convex-<cluster_id>-election
	bucketName := e.cfg.BucketName()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: bucketName,
		TTL:    e.cfg.LeaderTTL,
	})
	if err != nil {
		// Try to get existing bucket
		kv, err = js.KeyValue(ctx, bucketName)
		if err != nil {
			nc.Close()
			return fmt.Errorf("create/get KV bucket %s: %w", bucketName, err)
		}
	}
	e.kv = kv

	// Clear any stale cooldown for this node on startup
	cooldownKey := e.cooldownKey()
	_ = kv.Delete(ctx, cooldownKey)

	// Start KV watcher for real-time leader change detection
	watcher, err := kv.Watch(ctx, "leader")
	if err != nil {
		nc.Close()
		return fmt.Errorf("create leader watcher: %w", err)
	}
	e.watcher = watcher

	// Try to acquire leadership immediately
	e.tryAcquireOrRenew(ctx)

	// Start background goroutines
	e.wg.Add(2)
	go e.watchLoop(ctx)
	go e.heartbeatLoop(ctx)

	e.logger.Info("election started", "bucket", bucketName)
	return nil
}

// Stop stops the election process and releases leadership if held.
func (e *Election) Stop() {
	close(e.stopCh)
	e.wg.Wait()

	// Clean up watcher
	if e.watcher != nil {
		_ = e.watcher.Stop()
		e.watcher = nil
	}

	// Clean up leadership if we're the leader
	e.mu.Lock()
	if e.isLeader && e.kv != nil {
		_ = e.kv.Delete(context.Background(), "leader")
		e.isLeader = false
	}
	e.mu.Unlock()

	if e.nc != nil {
		e.nc.Close()
		e.nc = nil
	}

	e.logger.Info("election stopped")
}

// IsLeader returns true if this node is the current leader.
func (e *Election) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// CurrentLeader returns the ID of the current leader.
func (e *Election) CurrentLeader() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentLeader
}

// LeaderCh returns a channel that receives when this node becomes leader.
func (e *Election) LeaderCh() <-chan struct{} {
	return e.leaderCh
}

// StepDown voluntarily gives up leadership.
// It sets a cooldown marker to prevent immediate re-acquisition,
// allowing other nodes to take over.
func (e *Election) StepDown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Set cooldown marker BEFORE deleting leader key
	cooldownDuration := e.cfg.LeaderTTL
	cooldownExpiry := time.Now().Add(cooldownDuration).UnixMilli()
	cooldownKey := e.cooldownKey()

	// Store cooldown (will be auto-removed by KV TTL)
	_, err := e.kv.Create(ctx, cooldownKey, []byte(strconv.FormatInt(cooldownExpiry, 10)))
	if err != nil {
		// If key exists, update it
		_, err = e.kv.Put(ctx, cooldownKey, []byte(strconv.FormatInt(cooldownExpiry, 10)))
		if err != nil {
			e.logger.Warn("failed to set cooldown marker", "error", err)
		}
	}

	// Delete the leader key
	if err := e.kv.Delete(ctx, "leader"); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("delete leader key: %w", err)
	}

	// Update local state
	e.isLeader = false
	e.currentLeader = ""
	e.revision = 0

	e.logger.Info("stepped down from leadership", "cooldown", cooldownDuration)
	return nil
}

// cooldownKey returns the KV key for this node's cooldown marker.
func (e *Election) cooldownKey() string {
	return fmt.Sprintf("cooldown-%s", e.cfg.NodeID)
}

// watchLoop watches for leader changes via KV watcher.
// This provides real-time notification when the leader changes,
// eliminating the need for polling.
func (e *Election) watchLoop(ctx context.Context) {
	defer e.wg.Done()

	for {
		select {
		case <-e.stopCh:
			return
		case entry := <-e.watcher.Updates():
			if entry == nil {
				// Watcher closed or initial nil
				continue
			}
			e.handleLeaderUpdate(ctx, entry)
		}
	}
}

// handleLeaderUpdate processes a leader key update from the watcher.
func (e *Election) handleLeaderUpdate(ctx context.Context, entry jetstream.KeyValueEntry) {
	e.mu.Lock()
	defer e.mu.Unlock()

	operation := entry.Operation()

	switch operation {
	case jetstream.KeyValuePut:
		// Leader key was set or updated
		newLeader := string(entry.Value())
		oldLeader := e.currentLeader
		e.currentLeader = newLeader
		e.revision = entry.Revision()

		if newLeader == e.cfg.NodeID {
			// We are the leader
			if !e.isLeader {
				e.isLeader = true
				e.logger.Info("became leader via watch", "revision", entry.Revision())
				// Notify via channel (non-blocking)
				select {
				case e.leaderCh <- struct{}{}:
				default:
				}
			}
		} else {
			// Someone else is the leader
			if e.isLeader {
				e.logger.Warn("lost leadership to another node", "new_leader", newLeader)
				e.isLeader = false
			}
			if oldLeader != newLeader {
				e.logger.Info("leader changed", "old", oldLeader, "new", newLeader)
			}
		}

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		// Leader key was deleted (TTL expired or explicit delete)
		if e.currentLeader != "" {
			e.logger.Info("leader key deleted", "previous_leader", e.currentLeader)
		}
		e.currentLeader = ""
		e.revision = 0

		// If we were the leader and key was deleted externally, update state
		if e.isLeader {
			e.logger.Warn("leadership lost - key deleted")
			e.isLeader = false
		}

		// Try to acquire leadership (will be done in next heartbeat to avoid race)
	}
}

// heartbeatLoop periodically renews leadership or attempts acquisition.
func (e *Election) heartbeatLoop(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tryAcquireOrRenew(ctx)
		}
	}
}

// tryAcquireOrRenew attempts to acquire or renew leadership.
func (e *Election) tryAcquireOrRenew(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isLeader {
		e.renewLeadership(ctx)
	} else {
		e.tryAcquireLeadership(ctx)
	}
}

// tryAcquireLeadership attempts to become the leader.
func (e *Election) tryAcquireLeadership(ctx context.Context) {
	// Check current leader state first
	entry, err := e.kv.Get(ctx, "leader")
	if err == nil {
		// Leader exists
		existingLeader := string(entry.Value())
		e.currentLeader = existingLeader
		e.revision = entry.Revision()

		// If we're the leader in KV but not locally, reclaim
		if existingLeader == e.cfg.NodeID && !e.isLeader {
			e.logger.Info("reclaiming leadership", "revision", entry.Revision())
			e.isLeader = true
			select {
			case e.leaderCh <- struct{}{}:
			default:
			}
		}
		return
	}

	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		e.logger.Error("failed to get leader key", "error", err)
		return
	}

	// No leader - clear cached value
	e.currentLeader = ""

	// Check cooldown
	if e.isInCooldown(ctx) {
		e.logger.Debug("in cooldown, skipping acquisition")
		return
	}

	// Try to become leader using Create (CAS with revision 0)
	rev, err := e.kv.Create(ctx, "leader", []byte(e.cfg.NodeID))
	if err != nil {
		// Someone else may have created it
		if entry, getErr := e.kv.Get(ctx, "leader"); getErr == nil {
			e.currentLeader = string(entry.Value())
			e.revision = entry.Revision()
		}
		e.logger.Debug("failed to acquire leadership", "error", err)
		return
	}

	// Successfully became leader
	e.isLeader = true
	e.currentLeader = e.cfg.NodeID
	e.revision = rev
	e.renewFailures = 0

	e.logger.Info("acquired leadership", "revision", rev)

	// Notify via channel
	select {
	case e.leaderCh <- struct{}{}:
	default:
	}
}

// isInCooldown checks if this node recently stepped down.
func (e *Election) isInCooldown(ctx context.Context) bool {
	cooldownKey := e.cooldownKey()

	entry, err := e.kv.Get(ctx, cooldownKey)
	if err != nil {
		return false
	}

	expiryMs, err := strconv.ParseInt(string(entry.Value()), 10, 64)
	if err != nil {
		e.logger.Warn("invalid cooldown value", "value", string(entry.Value()))
		_ = e.kv.Delete(ctx, cooldownKey)
		return false
	}

	nowMs := time.Now().UnixMilli()
	if nowMs < expiryMs {
		e.logger.Debug("cooldown active", "remaining_ms", expiryMs-nowMs)
		return true
	}

	// Cooldown expired
	_ = e.kv.Delete(ctx, cooldownKey)
	return false
}

// maxRenewFailures is the number of consecutive renewal failures allowed.
const maxRenewFailures = 2

// renewLeadership renews the leadership lease.
func (e *Election) renewLeadership(ctx context.Context) {
	// Update the key to refresh TTL using CAS
	rev, err := e.kv.Update(ctx, "leader", []byte(e.cfg.NodeID), e.revision)
	if err != nil {
		e.renewFailures++
		e.logger.Warn("failed to renew leadership",
			"error", err,
			"consecutive_failures", e.renewFailures,
			"max_failures", maxRenewFailures,
			"is_transient", isTransientNATSError(err))

		// For transient errors, allow more failures before stepping down
		if isTransientNATSError(err) && e.renewFailures <= maxRenewFailures {
			e.logger.Info("transient error, staying leader while client reconnects",
				"failures", e.renewFailures)
			return
		}

		// Too many failures - step down
		if e.renewFailures > maxRenewFailures {
			e.logger.Warn("too many renewal failures, stepping down")
		}

		e.isLeader = false
		e.currentLeader = ""
		e.revision = 0
		e.renewFailures = 0
		return
	}

	// Success
	e.renewFailures = 0
	e.revision = rev
	e.logger.Debug("renewed leadership", "revision", rev)
}

// isTransientNATSError returns true if the error is likely transient.
func isTransientNATSError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Connection-level errors
	if errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrDisconnected) {
		return true
	}

	// JetStream cluster errors
	if errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, jetstream.ErrJetStreamNotEnabled) {
		return true
	}

	// Check for common transient error patterns
	transientPatterns := []string{
		"no responders",
		"timeout",
		"connection closed",
		"cluster not available",
		"meta leader",
		"raft",
		"context deadline",
		"context canceled",
	}

	for _, pattern := range transientPatterns {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}

	return false
}
