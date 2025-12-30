// Package cluster provides cluster management functionality including state machine and leader election.
package cluster

import (
	"errors"
	"sync"
)

// Role represents the role of a node in the cluster.
type Role int

const (
	// RolePassive indicates the node is a passive/standby node.
	RolePassive Role = iota
	// RolePrimary indicates the node is the active/leader node.
	RolePrimary
)

// String returns the string representation of the role.
func (r Role) String() string {
	switch r {
	case RolePassive:
		return "PASSIVE"
	case RolePrimary:
		return "PRIMARY"
	default:
		return "UNKNOWN"
	}
}

// Errors for state transitions.
var (
	ErrAlreadyLeader = errors.New("node is already the leader")
	ErrNotLeader     = errors.New("node is not the leader")
)

// State represents the current state of the cluster node.
type State struct {
	mu sync.RWMutex

	nodeID    string
	clusterID string
	role      Role
	leader    string
	hasVIP    bool

	// Callbacks
	onBecomeLeader  func()
	onStepDown      func()
	onLeaderChange  func(leader string)
}

// NewState creates a new cluster state.
func NewState(nodeID, clusterID string) *State {
	return &State{
		nodeID:    nodeID,
		clusterID: clusterID,
		role:      RolePassive,
	}
}

// NodeID returns the node ID.
func (s *State) NodeID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeID
}

// ClusterID returns the cluster ID.
func (s *State) ClusterID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterID
}

// Role returns the current role of the node.
func (s *State) Role() Role {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role
}

// Leader returns the current leader node ID.
func (s *State) Leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leader
}

// HasVIP returns whether this node currently holds the VIP.
func (s *State) HasVIP() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hasVIP
}

// IsLeader returns true if this node is the current leader.
func (s *State) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role == RolePrimary && s.leader == s.nodeID
}

// BecomeLeader transitions the node to the primary/leader role.
func (s *State) BecomeLeader() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role == RolePrimary {
		return ErrAlreadyLeader
	}

	s.role = RolePrimary
	s.leader = s.nodeID

	if s.onBecomeLeader != nil {
		s.onBecomeLeader()
	}

	return nil
}

// StepDown transitions the node from primary to passive role.
func (s *State) StepDown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != RolePrimary {
		return ErrNotLeader
	}

	s.role = RolePassive
	s.leader = ""

	if s.onStepDown != nil {
		s.onStepDown()
	}

	return nil
}

// SetLeader updates the current leader to another node.
func (s *State) SetLeader(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.leader = nodeID
	// If another node is leader, we must be passive
	if nodeID != s.nodeID {
		s.role = RolePassive
	}

	if s.onLeaderChange != nil {
		s.onLeaderChange(nodeID)
	}
}

// ClearLeader clears the current leader.
func (s *State) ClearLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.leader = ""
	// When there's no leader, we remain passive
	s.role = RolePassive

	if s.onLeaderChange != nil {
		s.onLeaderChange("")
	}
}

// SetVIPAcquired sets whether the VIP has been acquired.
func (s *State) SetVIPAcquired(acquired bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hasVIP = acquired
}

// OnBecomeLeader registers a callback for when this node becomes leader.
func (s *State) OnBecomeLeader(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onBecomeLeader = fn
}

// OnStepDown registers a callback for when this node steps down from leader.
func (s *State) OnStepDown(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onStepDown = fn
}

// OnLeaderChange registers a callback for when the leader changes.
func (s *State) OnLeaderChange(fn func(leader string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onLeaderChange = fn
}
