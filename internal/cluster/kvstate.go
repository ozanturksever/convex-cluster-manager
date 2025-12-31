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
	"github.com/ozanturksever/convex-cluster-manager/internal/natsutil"
)

// State management errors
var (
	ErrStateNotStarted = errors.New("state manager not started")
	ErrNodeNotFound    = errors.New("node not found")
)

// NodeState represents the persisted state of a cluster node.
type NodeState struct {
	NodeID        string    `json:"nodeId"`
	ClusterID     string    `json:"clusterId"`
	Role          string    `json:"role"`          // "PRIMARY" or "PASSIVE"
	BackendStatus string    `json:"backendStatus"` // "running", "stopped", "failed"
	LastSeen      time.Time `json:"lastSeen"`
	Version       string    `json:"version,omitempty"` // Software version
}

// StateEvent represents a state change event from the KV watcher.
type StateEvent struct {
	NodeID    string
	State     *NodeState // nil if node was deleted
	Operation string     // "put" or "delete"
}

// StateConfig contains configuration for the state manager.
type StateConfig struct {
	ClusterID       string
	NodeID          string
	NATSURLs        []string
	NATSCredentials string
	StateTTL        time.Duration // TTL for state entries (optional, 0 = no TTL)
}

// BucketName returns the auth-compatible KV bucket name for state.
// Format: convex-<cluster_id>-state
// This allows NATS auth rules like: $KV.convex-mycluster-state.>
func (c StateConfig) BucketName() string {
	return fmt.Sprintf("convex-%s-state", c.ClusterID)
}

// NodeKey returns the KV key for a specific node's state.
// Format: nodes.<node_id> (using dots for NATS wildcard compatibility)
func (c StateConfig) NodeKey(nodeID string) string {
	return fmt.Sprintf("nodes.%s", nodeID)
}

// StateManager manages cluster node state in NATS KV.
// It provides:
// - Persistent storage of node state
// - KV watchers for real-time state change notifications
// - Methods to get/set node state across the cluster
type StateManager struct {
	cfg    StateConfig
	logger *slog.Logger

	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue

	mu      sync.RWMutex
	running bool

	// Watcher management
	watcherMu sync.Mutex
	watchers  []jetstream.KeyWatcher
}

// NewStateManager creates a new state manager.
func NewStateManager(cfg StateConfig) (*StateManager, error) {
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("clusterID is required")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("nodeID is required")
	}
	if len(cfg.NATSURLs) == 0 {
		return nil, fmt.Errorf("at least one NATS URL is required")
	}

	return &StateManager{
		cfg:    cfg,
		logger: slog.Default().With("component", "kvstate", "node", cfg.NodeID, "cluster", cfg.ClusterID),
	}, nil
}

// Start connects to NATS and creates/gets the KV bucket.
func (sm *StateManager) Start(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.running {
		return nil
	}

	// Connect to NATS with automatic failover and cluster discovery
	nc, err := natsutil.Connect(natsutil.ConnectOptions{
		URLs:        sm.cfg.NATSURLs,
		Credentials: sm.cfg.NATSCredentials,
		Logger:      sm.logger,
	})
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	sm.nc = nc

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("create JetStream context: %w", err)
	}
	sm.js = js

	// Create or get KV bucket for state
	bucketName := sm.cfg.BucketName()
	kvConfig := jetstream.KeyValueConfig{
		Bucket:      bucketName,
		Description: fmt.Sprintf("Cluster state for %s", sm.cfg.ClusterID),
	}
	if sm.cfg.StateTTL > 0 {
		kvConfig.TTL = sm.cfg.StateTTL
	}

	kv, err := js.CreateKeyValue(ctx, kvConfig)
	if err != nil {
		// Try to get existing bucket
		kv, err = js.KeyValue(ctx, bucketName)
		if err != nil {
			nc.Close()
			return fmt.Errorf("create/get KV bucket %s: %w", bucketName, err)
		}
	}
	sm.kv = kv
	sm.running = true

	sm.logger.Info("state manager started", "bucket", bucketName)
	return nil
}

// Stop stops the state manager and closes the NATS connection.
func (sm *StateManager) Stop() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.running {
		return nil
	}

	// Stop all watchers
	sm.watcherMu.Lock()
	for _, w := range sm.watchers {
		_ = w.Stop()
	}
	sm.watchers = nil
	sm.watcherMu.Unlock()

	sm.running = false

	if sm.nc != nil {
		sm.nc.Close()
		sm.nc = nil
	}

	sm.logger.Info("state manager stopped")
	return nil
}

// Running returns true if the state manager is running.
func (sm *StateManager) Running() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.running
}

// SetNodeState sets the state for a node.
// If state is nil, the node's state is deleted.
func (sm *StateManager) SetNodeState(ctx context.Context, state *NodeState) error {
	sm.mu.RLock()
	kv := sm.kv
	running := sm.running
	sm.mu.RUnlock()

	if !running || kv == nil {
		return ErrStateNotStarted
	}

	key := sm.cfg.NodeKey(state.NodeID)

	// Update LastSeen timestamp
	state.LastSeen = time.Now()

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	_, err = kv.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("put state: %w", err)
	}

	sm.logger.Debug("set node state", "node", state.NodeID, "role", state.Role)
	return nil
}

// GetNodeState gets the state for a specific node.
func (sm *StateManager) GetNodeState(ctx context.Context, nodeID string) (*NodeState, error) {
	sm.mu.RLock()
	kv := sm.kv
	running := sm.running
	sm.mu.RUnlock()

	if !running || kv == nil {
		return nil, ErrStateNotStarted
	}

	key := sm.cfg.NodeKey(nodeID)

	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("get state: %w", err)
	}

	var state NodeState
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	return &state, nil
}

// DeleteNodeState deletes the state for a node.
func (sm *StateManager) DeleteNodeState(ctx context.Context, nodeID string) error {
	sm.mu.RLock()
	kv := sm.kv
	running := sm.running
	sm.mu.RUnlock()

	if !running || kv == nil {
		return ErrStateNotStarted
	}

	key := sm.cfg.NodeKey(nodeID)

	err := kv.Delete(ctx, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("delete state: %w", err)
	}

	sm.logger.Debug("deleted node state", "node", nodeID)
	return nil
}

// ListNodes returns the state of all nodes in the cluster.
func (sm *StateManager) ListNodes(ctx context.Context) (map[string]*NodeState, error) {
	sm.mu.RLock()
	kv := sm.kv
	running := sm.running
	sm.mu.RUnlock()

	if !running || kv == nil {
		return nil, ErrStateNotStarted
	}

	nodes := make(map[string]*NodeState)

	// List all keys under nodes/
	keys, err := kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nodes, nil
		}
		return nil, fmt.Errorf("list keys: %w", err)
	}

	for _, key := range keys {
		// Only process node keys (format: nodes.<node_id>)
		if len(key) <= 6 || key[:6] != "nodes." {
			continue
		}

		entry, err := kv.Get(ctx, key)
		if err != nil {
			sm.logger.Warn("failed to get node state", "key", key, "error", err)
			continue
		}

		var state NodeState
		if err := json.Unmarshal(entry.Value(), &state); err != nil {
			sm.logger.Warn("failed to unmarshal node state", "key", key, "error", err)
			continue
		}

		nodes[state.NodeID] = &state
	}

	return nodes, nil
}

// WatchNodes returns a channel that receives state change events for all nodes.
// The channel is closed when the context is cancelled or Stop() is called.
func (sm *StateManager) WatchNodes(ctx context.Context) (<-chan StateEvent, error) {
	sm.mu.RLock()
	kv := sm.kv
	running := sm.running
	sm.mu.RUnlock()

	if !running || kv == nil {
		return nil, ErrStateNotStarted
	}

	// Watch all keys under nodes. using wildcard
	// NATS KV uses * for single-level wildcard matching
	watcher, err := kv.Watch(ctx, "nodes.*")
	if err != nil {
		return nil, fmt.Errorf("create watcher: %w", err)
	}

	// Track watcher for cleanup
	sm.watcherMu.Lock()
	sm.watchers = append(sm.watchers, watcher)
	sm.watcherMu.Unlock()

	// Create output channel
	events := make(chan StateEvent, 16)

	// Start goroutine to process watcher updates
	go func() {
		defer close(events)
		defer func() {
			// Remove watcher from tracking
			sm.watcherMu.Lock()
			for i, w := range sm.watchers {
				if w == watcher {
					sm.watchers = append(sm.watchers[:i], sm.watchers[i+1:]...)
					break
				}
			}
			sm.watcherMu.Unlock()
			_ = watcher.Stop()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-watcher.Updates():
				if entry == nil {
					// Initial nil or watcher closed
					continue
				}

				event := sm.processWatchEntry(entry)
				if event != nil {
					select {
					case events <- *event:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return events, nil
}

// processWatchEntry converts a KV entry to a StateEvent.
func (sm *StateManager) processWatchEntry(entry jetstream.KeyValueEntry) *StateEvent {
	key := entry.Key()

	// Extract node ID from key (nodes.<node_id>)
	if len(key) <= 6 || key[:6] != "nodes." {
		return nil
	}
	nodeID := key[6:]

	event := &StateEvent{
		NodeID: nodeID,
	}

	switch entry.Operation() {
	case jetstream.KeyValuePut:
		event.Operation = "put"
		var state NodeState
		if err := json.Unmarshal(entry.Value(), &state); err != nil {
			sm.logger.Warn("failed to unmarshal watch event", "key", key, "error", err)
			return nil
		}
		event.State = &state

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		event.Operation = "delete"
		event.State = nil

	default:
		return nil
	}

	return event
}

// WatchNode returns a channel that receives state change events for a specific node.
// The channel is closed when the context is cancelled or Stop() is called.
func (sm *StateManager) WatchNode(ctx context.Context, nodeID string) (<-chan StateEvent, error) {
	sm.mu.RLock()
	kv := sm.kv
	running := sm.running
	sm.mu.RUnlock()

	if !running || kv == nil {
		return nil, ErrStateNotStarted
	}

	key := sm.cfg.NodeKey(nodeID)

	watcher, err := kv.Watch(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("create watcher for node %s: %w", nodeID, err)
	}

	// Track watcher for cleanup
	sm.watcherMu.Lock()
	sm.watchers = append(sm.watchers, watcher)
	sm.watcherMu.Unlock()

	// Create output channel
	events := make(chan StateEvent, 8)

	// Start goroutine to process watcher updates
	go func() {
		defer close(events)
		defer func() {
			// Remove watcher from tracking
			sm.watcherMu.Lock()
			for i, w := range sm.watchers {
				if w == watcher {
					sm.watchers = append(sm.watchers[:i], sm.watchers[i+1:]...)
					break
				}
			}
			sm.watcherMu.Unlock()
			_ = watcher.Stop()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-watcher.Updates():
				if entry == nil {
					continue
				}

				event := &StateEvent{
					NodeID: nodeID,
				}

				switch entry.Operation() {
				case jetstream.KeyValuePut:
					event.Operation = "put"
					var state NodeState
					if err := json.Unmarshal(entry.Value(), &state); err != nil {
						sm.logger.Warn("failed to unmarshal watch event", "node", nodeID, "error", err)
						continue
					}
					event.State = &state

				case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
					event.Operation = "delete"
					event.State = nil

				default:
					continue
				}

				select {
				case events <- *event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return events, nil
}

// UpdateOwnState is a convenience method to update this node's state.
func (sm *StateManager) UpdateOwnState(ctx context.Context, role, backendStatus string) error {
	state := &NodeState{
		NodeID:        sm.cfg.NodeID,
		ClusterID:     sm.cfg.ClusterID,
		Role:          role,
		BackendStatus: backendStatus,
	}
	return sm.SetNodeState(ctx, state)
}
