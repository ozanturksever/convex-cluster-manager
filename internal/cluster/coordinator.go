package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Coordinator errors
var (
	ErrLeaseExpired    = errors.New("lease expired")
	ErrEpochMismatch   = errors.New("epoch mismatch - another node took over")
	ErrNotPrimary      = errors.New("not the primary")
	ErrAlreadyPrimary  = errors.New("already primary")
	ErrCASFailed       = errors.New("compare-and-swap failed")
)

// Note: Role, RolePrimary, RolePassive are defined in state.go

// Lease represents a leadership lease with epoch-based fencing.
type Lease struct {
	// DBID identifies the database being replicated.
	DBID string `json:"db_id"`
	
	// NodeID is the node holding the lease.
	NodeID string `json:"node_id"`
	
	// Epoch is a monotonically increasing counter for split-brain prevention.
	// Each time a new primary takes over, the epoch is incremented.
	Epoch int64 `json:"epoch"`
	
	// ExpiresAt is when the lease expires if not renewed.
	ExpiresAt time.Time `json:"expires_at"`
	
	// AcquiredAt is when the lease was first acquired in this epoch.
	AcquiredAt time.Time `json:"acquired_at"`
	
	// Revision is the NATS KV revision for CAS operations.
	Revision uint64 `json:"-"`
}

// IsExpired returns true if the lease has expired.
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// CoordinatorConfig configures the cluster coordinator.
type CoordinatorConfig struct {
	ClusterID  string
	NodeID     string
	DBID       string        // Database identifier for multi-db support
	NATSURLs   []string
	NATSCreds  string
	LeaseTTL   time.Duration // How long a lease is valid (default: 10s)
	RenewEvery time.Duration // How often to renew (default: 3s, should be < TTL/3)
}

// Coordinator manages leader election with epoch-based split-brain prevention.
// It uses NATS KV as the coordination backend.
type Coordinator struct {
	cfg    CoordinatorConfig
	logger *slog.Logger

	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue

	mu            sync.RWMutex
	lease         *Lease
	localRole     Role
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// Callbacks
	onBecomeLeader func()
	onLoseLeader   func()
	onLeaderChange func(nodeID string)
}

// NewCoordinator creates a new cluster coordinator.
func NewCoordinator(cfg CoordinatorConfig) (*Coordinator, error) {
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
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 10 * time.Second
	}
	if cfg.RenewEvery == 0 {
		cfg.RenewEvery = 3 * time.Second
	}
	if cfg.DBID == "" {
		cfg.DBID = "default"
	}

	return &Coordinator{
		cfg:       cfg,
		logger:    slog.Default().With("component", "coordinator", "node", cfg.NodeID),
		localRole: RolePassive,
		stopCh:    make(chan struct{}),
	}, nil
}

// OnBecomeLeader sets a callback for when this node becomes the leader.
func (c *Coordinator) OnBecomeLeader(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onBecomeLeader = fn
}

// OnLoseLeader sets a callback for when this node loses leadership.
func (c *Coordinator) OnLoseLeader(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onLoseLeader = fn
}

// OnLeaderChange sets a callback for when the leader changes.
func (c *Coordinator) OnLeaderChange(fn func(nodeID string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onLeaderChange = fn
}

// Start connects to NATS and begins the leader election loop.
func (c *Coordinator) Start(ctx context.Context) error {
	// Reset stop channel for restart capability
	c.stopCh = make(chan struct{})

	// Connect to NATS
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
	}
	if c.cfg.NATSCreds != "" {
		opts = append(opts, nats.UserCredentials(c.cfg.NATSCreds))
	}

	nc, err := nats.Connect(c.cfg.NATSURLs[0], opts...)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	c.nc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("create JetStream: %w", err)
	}
	c.js = js

	// Create or get KV bucket
	bucketName := fmt.Sprintf("cluster-%s-coord", c.cfg.ClusterID)
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: bucketName,
		TTL:    c.cfg.LeaseTTL * 2, // TTL is longer than lease to allow for detection
	})
	if err != nil {
		// Try to get existing bucket
		kv, err = js.KeyValue(ctx, bucketName)
		if err != nil {
			nc.Close()
			return fmt.Errorf("create/get KV bucket: %w", err)
		}
	}
	c.kv = kv

	// Try to acquire leadership immediately
	c.tryAcquireOrRenew(ctx)

	// Start election loop
	c.wg.Add(1)
	go c.electionLoop(ctx)

	return nil
}

// Stop stops the coordinator and releases any held lease.
// Note: We do NOT delete the lease here - it will expire naturally via TTL.
// This allows the next leader to properly increment the epoch.
func (c *Coordinator) Stop() {
	close(c.stopCh)
	c.wg.Wait()

	c.mu.Lock()
	c.lease = nil
	c.localRole = RolePassive
	c.mu.Unlock()

	if c.nc != nil {
		c.nc.Close()
	}
}

// Role returns the current role of this node.
func (c *Coordinator) Role() Role {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localRole
}

// Leader returns the current leader's node ID, or empty if unknown.
func (c *Coordinator) Leader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lease != nil {
		return c.lease.NodeID
	}
	return ""
}

// Epoch returns the current epoch, or 0 if unknown.
func (c *Coordinator) Epoch() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lease != nil {
		return c.lease.Epoch
	}
	return 0
}

// TryAcquire attempts to acquire leadership with epoch fencing.
// Returns ErrAlreadyPrimary if already the leader.
// Returns ErrCASFailed if another node acquired the lease.
func (c *Coordinator) TryAcquire(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.localRole == RolePrimary && c.lease != nil && !c.lease.IsExpired() {
		return ErrAlreadyPrimary
	}

	return c.tryAcquireLocked(ctx)
}

// Renew renews the current lease if we're the leader.
// Returns ErrNotPrimary if not the leader.
// Returns ErrEpochMismatch if another node took over.
func (c *Coordinator) Renew(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.localRole != RolePrimary || c.lease == nil {
		return ErrNotPrimary
	}

	return c.renewLeaseLocked(ctx)
}

// StepDown voluntarily gives up leadership.
func (c *Coordinator) StepDown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.localRole != RolePrimary {
		return ErrNotPrimary
	}

	// Delete the lease
	if err := c.kv.Delete(ctx, c.leaseKey()); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("delete lease: %w", err)
	}

	c.localRole = RolePassive
	c.lease = nil

	c.logger.Info("stepped down from leadership")

	if c.onLoseLeader != nil {
		go c.onLoseLeader()
	}

	return nil
}

// leaseKey returns the KV key for the lease.
func (c *Coordinator) leaseKey() string {
	return fmt.Sprintf("lease/%s", c.cfg.DBID)
}

// electionLoop runs the periodic lease acquisition/renewal.
func (c *Coordinator) electionLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cfg.RenewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.tryAcquireOrRenew(ctx)
		}
	}
}

// tryAcquireOrRenew attempts to acquire or renew the lease.
func (c *Coordinator) tryAcquireOrRenew(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.localRole == RolePrimary {
		// Try to renew
		if err := c.renewLeaseLocked(ctx); err != nil {
			c.logger.Warn("failed to renew lease", "error", err)
			c.handleLostLeadership()
		}
	} else {
		// Try to acquire
		if err := c.tryAcquireLocked(ctx); err != nil {
			if !errors.Is(err, ErrCASFailed) {
				c.logger.Debug("failed to acquire lease", "error", err)
			}
		}
	}
}

// tryAcquireLocked attempts to acquire the lease. Must be called with mu held.
func (c *Coordinator) tryAcquireLocked(ctx context.Context) error {
	key := c.leaseKey()

	// Check current lease state
	entry, err := c.kv.Get(ctx, key)
	if err == nil {
		// Lease exists - check if expired
		var existingLease Lease
		if err := json.Unmarshal(entry.Value(), &existingLease); err != nil {
			return fmt.Errorf("unmarshal lease: %w", err)
		}
		existingLease.Revision = entry.Revision()

		// Update leader info for callbacks
		oldLeader := ""
		if c.lease != nil {
			oldLeader = c.lease.NodeID
		}
		c.lease = &existingLease

		if existingLease.NodeID != c.cfg.NodeID && !existingLease.IsExpired() {
			// Another node holds a valid lease
			if oldLeader != existingLease.NodeID && c.onLeaderChange != nil {
				go c.onLeaderChange(existingLease.NodeID)
			}
			return ErrCASFailed
		}

		// Lease expired or we own it - try to take over with incremented epoch
		newEpoch := existingLease.Epoch + 1
		if existingLease.NodeID == c.cfg.NodeID {
			newEpoch = existingLease.Epoch // Same epoch if we're reclaiming
		}
		return c.createLease(ctx, newEpoch, entry.Revision())
	}

	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("get lease: %w", err)
	}

	// No lease exists - create new one with epoch 1
	return c.createLease(ctx, 1, 0)
}

// createLease creates a new lease. Must be called with mu held.
func (c *Coordinator) createLease(ctx context.Context, epoch int64, expectedRevision uint64) error {
	now := time.Now()
	lease := Lease{
		DBID:       c.cfg.DBID,
		NodeID:     c.cfg.NodeID,
		Epoch:      epoch,
		ExpiresAt:  now.Add(c.cfg.LeaseTTL),
		AcquiredAt: now,
	}

	data, err := json.Marshal(lease)
	if err != nil {
		return fmt.Errorf("marshal lease: %w", err)
	}

	var rev uint64
	if expectedRevision == 0 {
		// Create new key
		rev, err = c.kv.Create(ctx, c.leaseKey(), data)
	} else {
		// Update existing key with CAS
		rev, err = c.kv.Update(ctx, c.leaseKey(), data, expectedRevision)
	}

	if err != nil {
		return ErrCASFailed
	}

	lease.Revision = rev
	wasPassive := c.localRole == RolePassive
	c.lease = &lease
	c.localRole = RolePrimary

	c.logger.Info("acquired leadership", "epoch", epoch, "revision", rev)

	if wasPassive && c.onBecomeLeader != nil {
		go c.onBecomeLeader()
	}

	return nil
}

// renewLeaseLocked renews the current lease. Must be called with mu held.
func (c *Coordinator) renewLeaseLocked(ctx context.Context) error {
	if c.lease == nil {
		return ErrNotPrimary
	}

	// First verify our lease is still valid in KV
	entry, err := c.kv.Get(ctx, c.leaseKey())
	if err != nil {
		return fmt.Errorf("get lease for renewal: %w", err)
	}

	var currentLease Lease
	if err := json.Unmarshal(entry.Value(), &currentLease); err != nil {
		return fmt.Errorf("unmarshal lease: %w", err)
	}

	// Check for epoch mismatch (split-brain prevention)
	if currentLease.Epoch != c.lease.Epoch || currentLease.NodeID != c.cfg.NodeID {
		c.handleLostLeadership()
		return ErrEpochMismatch
	}

	// Renew by updating expiry
	c.lease.ExpiresAt = time.Now().Add(c.cfg.LeaseTTL)
	c.lease.Revision = entry.Revision()

	data, err := json.Marshal(c.lease)
	if err != nil {
		return fmt.Errorf("marshal lease: %w", err)
	}

	rev, err := c.kv.Update(ctx, c.leaseKey(), data, entry.Revision())
	if err != nil {
		c.handleLostLeadership()
		return fmt.Errorf("update lease: %w", err)
	}

	c.lease.Revision = rev
	c.logger.Debug("renewed lease", "epoch", c.lease.Epoch, "revision", rev)

	return nil
}

// handleLostLeadership handles the transition from primary to passive.
// Must be called with mu held.
func (c *Coordinator) handleLostLeadership() {
	if c.localRole != RolePrimary {
		return
	}

	c.localRole = RolePassive
	c.logger.Warn("lost leadership")

	if c.onLoseLeader != nil {
		go c.onLoseLeader()
	}
}
