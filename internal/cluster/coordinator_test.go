package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"
)

func TestCoordinator_SingleNode_AcquiresLeadership(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	cfg := CoordinatorConfig{
		ClusterID:  "test-single",
		NodeID:     "node-1",
		DBID:       "testdb",
		NATSURLs:   []string{natsContainer.URL},
		LeaseTTL:   5 * time.Second,
		RenewEvery: 1 * time.Second,
	}

	coord, err := NewCoordinator(cfg)
	require.NoError(t, err)

	becameLeader := make(chan struct{}, 1)
	coord.OnBecomeLeader(func() {
		becameLeader <- struct{}{}
	})

	err = coord.Start(ctx)
	require.NoError(t, err)
	defer coord.Stop()

	// Wait for leadership
	select {
	case <-becameLeader:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for leadership")
	}

	assert.Equal(t, RolePrimary, coord.Role())
	assert.Equal(t, "node-1", coord.Leader())
	assert.Equal(t, int64(1), coord.Epoch())
}

func TestCoordinator_TwoNodes_OnlyOneLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	baseCfg := CoordinatorConfig{
		ClusterID:  "test-two-nodes",
		DBID:       "testdb",
		NATSURLs:   []string{natsContainer.URL},
		LeaseTTL:   5 * time.Second,
		RenewEvery: 1 * time.Second,
	}

	cfg1 := baseCfg
	cfg1.NodeID = "node-1"

	cfg2 := baseCfg
	cfg2.NodeID = "node-2"

	coord1, err := NewCoordinator(cfg1)
	require.NoError(t, err)

	coord2, err := NewCoordinator(cfg2)
	require.NoError(t, err)

	// Track who becomes leader
	var leaderMu sync.Mutex
	leaders := make(map[string]bool)

	coord1.OnBecomeLeader(func() {
		leaderMu.Lock()
		leaders["node-1"] = true
		leaderMu.Unlock()
	})

	coord2.OnBecomeLeader(func() {
		leaderMu.Lock()
		leaders["node-2"] = true
		leaderMu.Unlock()
	})

	// Start both coordinators
	err = coord1.Start(ctx)
	require.NoError(t, err)
	defer coord1.Stop()

	err = coord2.Start(ctx)
	require.NoError(t, err)
	defer coord2.Stop()

	// Wait for election to settle
	time.Sleep(3 * time.Second)

	// Verify exactly one leader
	leaderMu.Lock()
	leaderCount := 0
	for _, isLeader := range leaders {
		if isLeader {
			leaderCount++
		}
	}
	leaderMu.Unlock()

	// Check roles
	role1 := coord1.Role()
	role2 := coord2.Role()

	t.Logf("coord1 role: %s, coord2 role: %s", role1, role2)

	// Exactly one should be PRIMARY
	primaryCount := 0
	if role1 == RolePrimary {
		primaryCount++
	}
	if role2 == RolePrimary {
		primaryCount++
	}

	assert.Equal(t, 1, primaryCount, "exactly one node should be PRIMARY")

	// Both should agree on the leader
	leader1 := coord1.Leader()
	leader2 := coord2.Leader()
	assert.Equal(t, leader1, leader2, "both nodes should agree on leader")
}

func TestCoordinator_LeaderFailover_EpochIncreases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	baseCfg := CoordinatorConfig{
		ClusterID:  "test-failover-epoch",
		DBID:       "testdb",
		NATSURLs:   []string{natsContainer.URL},
		LeaseTTL:   3 * time.Second, // Short TTL for faster test
		RenewEvery: 1 * time.Second,
	}

	cfg1 := baseCfg
	cfg1.NodeID = "node-1"

	cfg2 := baseCfg
	cfg2.NodeID = "node-2"

	coord1, err := NewCoordinator(cfg1)
	require.NoError(t, err)

	coord2, err := NewCoordinator(cfg2)
	require.NoError(t, err)

	// Start node 1 first
	err = coord1.Start(ctx)
	require.NoError(t, err)

	// Wait for node 1 to become leader
	time.Sleep(2 * time.Second)
	require.Equal(t, RolePrimary, coord1.Role(), "node-1 should be leader")

	initialEpoch := coord1.Epoch()
	t.Logf("Initial epoch: %d", initialEpoch)

	// Start node 2 as standby
	err = coord2.Start(ctx)
	require.NoError(t, err)
	defer coord2.Stop()

	// Verify node 2 is passive
	time.Sleep(1 * time.Second)
	assert.Equal(t, RolePassive, coord2.Role(), "node-2 should be passive")

	// Stop node 1 to trigger failover
	t.Log("Stopping node-1 to trigger failover...")
	coord1.Stop()

	// Wait for node 2 to become leader
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if coord2.Role() == RolePrimary {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, RolePrimary, coord2.Role(), "node-2 should become leader after failover")

	// Verify epoch increased
	newEpoch := coord2.Epoch()
	t.Logf("New epoch after failover: %d", newEpoch)
	assert.Greater(t, newEpoch, initialEpoch, "epoch should increase after failover")
}

func TestCoordinator_StepDown_ReleasesLeadership(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	cfg := CoordinatorConfig{
		ClusterID:  "test-stepdown",
		NodeID:     "node-1",
		DBID:       "testdb",
		NATSURLs:   []string{natsContainer.URL},
		LeaseTTL:   5 * time.Second,
		RenewEvery: 1 * time.Second,
	}

	coord, err := NewCoordinator(cfg)
	require.NoError(t, err)

	lostLeader := make(chan struct{}, 1)
	coord.OnLoseLeader(func() {
		lostLeader <- struct{}{}
	})

	err = coord.Start(ctx)
	require.NoError(t, err)
	defer coord.Stop()

	// Wait to become leader
	time.Sleep(2 * time.Second)
	require.Equal(t, RolePrimary, coord.Role())

	// Step down
	err = coord.StepDown(ctx)
	require.NoError(t, err)

	// Verify callback was called
	select {
	case <-lostLeader:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for lostLeader callback")
	}

	assert.Equal(t, RolePassive, coord.Role())
}

func TestCoordinator_EpochMismatch_PreventsSplitBrain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	baseCfg := CoordinatorConfig{
		ClusterID:  "test-splitbrain",
		DBID:       "testdb",
		NATSURLs:   []string{natsContainer.URL},
		LeaseTTL:   3 * time.Second,
		RenewEvery: 1 * time.Second,
	}

	cfg1 := baseCfg
	cfg1.NodeID = "node-1"

	cfg2 := baseCfg
	cfg2.NodeID = "node-2"

	coord1, err := NewCoordinator(cfg1)
	require.NoError(t, err)

	coord2, err := NewCoordinator(cfg2)
	require.NoError(t, err)

	node1LostLeader := make(chan struct{}, 1)
	coord1.OnLoseLeader(func() {
		node1LostLeader <- struct{}{}
	})

	// Start node 1 as leader
	err = coord1.Start(ctx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)
	require.Equal(t, RolePrimary, coord1.Role())
	epoch1 := coord1.Epoch()

	// Stop coord1's renewal loop but keep it thinking it's leader
	// (simulating network partition)
	coord1.Stop()

	// Wait for TTL to expire
	time.Sleep(4 * time.Second)

	// Start node 2 which should take over
	err = coord2.Start(ctx)
	require.NoError(t, err)
	defer coord2.Stop()

	// Wait for node 2 to become leader
	time.Sleep(2 * time.Second)
	require.Equal(t, RolePrimary, coord2.Role())

	epoch2 := coord2.Epoch()
	t.Logf("epoch1=%d, epoch2=%d", epoch1, epoch2)

	// Verify epoch increased (split-brain prevention)
	assert.Greater(t, epoch2, epoch1, "new leader should have higher epoch")
}

func TestCoordinator_RenewPreventsTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	natsContainer, err := testutil.StartNATSContainer(ctx)
	require.NoError(t, err)
	defer func() { _ = natsContainer.Stop(ctx) }()

	cfg := CoordinatorConfig{
		ClusterID:  "test-renew",
		NodeID:     "node-1",
		DBID:       "testdb",
		NATSURLs:   []string{natsContainer.URL},
		LeaseTTL:   3 * time.Second, // Short TTL
		RenewEvery: 500 * time.Millisecond, // Fast renewal
	}

	coord, err := NewCoordinator(cfg)
	require.NoError(t, err)

	lostLeader := make(chan struct{}, 1)
	coord.OnLoseLeader(func() {
		lostLeader <- struct{}{}
	})

	err = coord.Start(ctx)
	require.NoError(t, err)
	defer coord.Stop()

	// Wait to become leader
	time.Sleep(2 * time.Second)
	require.Equal(t, RolePrimary, coord.Role())

	// Wait longer than TTL - should still be leader due to renewals
	time.Sleep(5 * time.Second)

	// Should not have lost leadership
	select {
	case <-lostLeader:
		t.Fatal("should not have lost leadership with continuous renewal")
	default:
		// Good
	}

	assert.Equal(t, RolePrimary, coord.Role())
}
