package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// Election manages leader election using NATS KV store.
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

	leaderCh chan struct{}
	stopCh   chan struct{}
	wg       sync.WaitGroup
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
		logger:   slog.Default().With("component", "election", "node", cfg.NodeID),
		leaderCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}, nil
}

// Start begins the election process.
func (e *Election) Start(ctx context.Context) error {
	// Reset stop channel for restart capability
	e.stopCh = make(chan struct{})

	// Connect to NATS
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
	}

	if e.cfg.NATSCredentials != "" {
		opts = append(opts, nats.UserCredentials(e.cfg.NATSCredentials))
	}

	nc, err := nats.Connect(e.cfg.NATSURLs[0], opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	e.nc = nc

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	e.js = js

	// Create or get KV bucket for leader election
	bucketName := fmt.Sprintf("cluster-%s-election", e.cfg.ClusterID)
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: bucketName,
		TTL:    e.cfg.LeaderTTL,
	})
	if err != nil {
		// Try to get existing bucket
		kv, err = js.KeyValue(ctx, bucketName)
		if err != nil {
			nc.Close()
			return fmt.Errorf("failed to create/get KV bucket: %w", err)
		}
	}
	e.kv = kv

	// Clear any stale cooldown for this node on startup.
	// A fresh daemon start indicates a new session - old cooldowns are no longer relevant.
	// This prevents cross-contamination between daemon restarts and test runs.
	cooldownKey := fmt.Sprintf("cooldown-%s", e.cfg.NodeID)
	_ = kv.Delete(ctx, cooldownKey)

	// Try to acquire leadership immediately
	e.tryAcquireOrRenew(ctx)

	// Start election loop
	e.wg.Add(1)
	go e.electionLoop(ctx)

	return nil
}

// Stop stops the election process.
func (e *Election) Stop() {
	close(e.stopCh)
	e.wg.Wait()

	if e.nc != nil {
		e.nc.Close()
	}
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
// It sets a cooldown marker in KV to prevent this node from immediately
// re-acquiring leadership, allowing other nodes to take over.
//
// This method can be called from:
// 1. The daemon's Election instance (where isLeader is true)
// 2. A CLI command's Election instance (where isLeader is false, but we still
//    need to delete the leader key and set cooldown)
//
// In both cases, we proceed with deleting the leader key and setting cooldown.
func (e *Election) StepDown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Set a cooldown marker BEFORE deleting the leader key.
	// This prevents this node from immediately re-acquiring leadership.
	// The cooldown duration equals the leader TTL, which is sufficient time
	// for other nodes to detect the missing leader and acquire leadership.
	// Note: The cooldown key will be auto-removed by the KV bucket's TTL.
	cooldownDuration := e.cfg.LeaderTTL
	cooldownExpiry := time.Now().Add(cooldownDuration).UnixMilli()
	cooldownKey := fmt.Sprintf("cooldown-%s", e.cfg.NodeID)
	
	// Store cooldown with TTL so it auto-expires
	_, err := e.kv.Create(ctx, cooldownKey, []byte(strconv.FormatInt(cooldownExpiry, 10)))
	if err != nil {
		// If key already exists, update it
		_, err = e.kv.Put(ctx, cooldownKey, []byte(strconv.FormatInt(cooldownExpiry, 10)))
		if err != nil {
			e.logger.Warn("failed to set cooldown marker", "error", err)
			// Continue with step down anyway
		}
	}

	// Delete the leader key
	err = e.kv.Delete(ctx, "leader")
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("failed to delete leader key: %w", err)
	}

	// Update local state (may already be false if called from CLI)
	e.isLeader = false
	e.currentLeader = ""
	e.revision = 0

	e.logger.Info("stepped down from leadership", "cooldown", cooldownDuration)
	return nil
}

func (e *Election) electionLoop(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			// Clean up leadership if we're the leader
			e.mu.Lock()
			if e.isLeader {
				_ = e.kv.Delete(context.Background(), "leader")
				e.isLeader = false
			}
			e.mu.Unlock()
			return
		case <-ticker.C:
			e.tryAcquireOrRenew(ctx)
		}
	}
}

func (e *Election) tryAcquireOrRenew(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isLeader {
		// Try to renew leadership
		e.renewLeadership(ctx)
	} else {
		// Try to acquire leadership
		e.tryAcquireLeadership(ctx)
	}
}

func (e *Election) tryAcquireLeadership(ctx context.Context) {
	// ALWAYS check and update currentLeader first, regardless of cooldown.
	// This ensures the cached leader value is accurate for health reporting.
	entry, err := e.kv.Get(ctx, "leader")
	if err == nil {
		// There's an existing leader
		existingLeader := string(entry.Value())
		e.currentLeader = existingLeader
		
		// IMPORTANT: If the existing leader is THIS node but we don't think we're
		// the leader (e.g., after a failed renewLeadership or daemon restart),
		// we need to reclaim leadership. This fixes the race condition where
		// isLeader=false but currentLeader=thisNode, causing the daemon to
		// stay PASSIVE when it should be PRIMARY.
		if existingLeader == e.cfg.NodeID && !e.isLeader {
			e.logger.Info("reclaiming leadership - leader key contains our node ID", 
				"revision", entry.Revision())
			e.isLeader = true
			e.revision = entry.Revision()
			
			// Notify through channel (non-blocking)
			select {
			case e.leaderCh <- struct{}{}:
			default:
			}
			return
		}
		
		e.logger.Debug("existing leader found", "leader", e.currentLeader)
		return
	}

	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		e.logger.Error("failed to get leader key", "error", err)
		return
	}

	// No leader exists - clear the cached leader value
	e.currentLeader = ""

	// Check if this node is in cooldown (recently stepped down).
	// Cooldown only prevents acquiring leadership, not knowing the leader state.
	if e.isInCooldown(ctx) {
		e.logger.Debug("in step-down cooldown, skipping leadership acquisition")
		return
	}

	// No leader exists and not in cooldown, try to become leader
	rev, err := e.kv.Create(ctx, "leader", []byte(e.cfg.NodeID))
	if err != nil {
		// Someone else may have created it
		entry, getErr := e.kv.Get(ctx, "leader")
		if getErr == nil {
			e.currentLeader = string(entry.Value())
		}
		e.logger.Debug("failed to acquire leadership", "error", err)
		return
	}

	// Successfully became leader
	e.isLeader = true
	e.currentLeader = e.cfg.NodeID
	e.revision = rev

	e.logger.Info("acquired leadership", "revision", rev)

	// Notify through channel (non-blocking)
	select {
	case e.leaderCh <- struct{}{}:
	default:
	}
}

// isInCooldown checks if this node recently stepped down and should not
// attempt to acquire leadership yet. This allows other nodes time to
// take over leadership during graceful failover.
func (e *Election) isInCooldown(ctx context.Context) bool {
	cooldownKey := fmt.Sprintf("cooldown-%s", e.cfg.NodeID)
	
	entry, err := e.kv.Get(ctx, cooldownKey)
	if err != nil {
		// No cooldown key exists or error reading it
		return false
	}
	
	// Parse the expiry timestamp
	expiryMs, err := strconv.ParseInt(string(entry.Value()), 10, 64)
	if err != nil {
		e.logger.Warn("invalid cooldown value", "value", string(entry.Value()))
		// Delete invalid cooldown key
		_ = e.kv.Delete(ctx, cooldownKey)
		return false
	}
	
	nowMs := time.Now().UnixMilli()
	if nowMs < expiryMs {
		// Still in cooldown
		remainingMs := expiryMs - nowMs
		e.logger.Debug("cooldown active", "remaining_ms", remainingMs)
		return true
	}
	
	// Cooldown expired, clean up the key
	_ = e.kv.Delete(ctx, cooldownKey)
	return false
}

func (e *Election) renewLeadership(ctx context.Context) {
	// Update the key to refresh TTL
	rev, err := e.kv.Update(ctx, "leader", []byte(e.cfg.NodeID), e.revision)
	if err != nil {
		e.logger.Warn("failed to renew leadership", "error", err)
		e.isLeader = false
		e.currentLeader = ""
		e.revision = 0
		return
	}

	e.revision = rev
	e.logger.Debug("renewed leadership", "revision", rev)
}
