package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
func (e *Election) StepDown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isLeader {
		return nil
	}

	// Delete the leader key
	err := e.kv.Delete(ctx, "leader")
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("failed to delete leader key: %w", err)
	}

	e.isLeader = false
	e.currentLeader = ""
	e.revision = 0

	e.logger.Info("stepped down from leadership")
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
	// Check if there's an existing leader
	entry, err := e.kv.Get(ctx, "leader")
	if err == nil {
		// There's an existing leader
		e.currentLeader = string(entry.Value())
		e.logger.Debug("existing leader found", "leader", e.currentLeader)
		return
	}

	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		e.logger.Error("failed to get leader key", "error", err)
		return
	}

	// No leader exists, try to become leader
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
