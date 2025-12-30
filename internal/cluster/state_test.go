package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRole(t *testing.T) {
	t.Run("role string representation", func(t *testing.T) {
		assert.Equal(t, "PASSIVE", RolePassive.String())
		assert.Equal(t, "PRIMARY", RolePrimary.String())
		assert.Equal(t, "UNKNOWN", Role(99).String())
	})
}

func TestNewState(t *testing.T) {
	t.Run("creates state with initial values", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		assert.Equal(t, "node-1", state.NodeID())
		assert.Equal(t, "cluster-1", state.ClusterID())
		assert.Equal(t, RolePassive, state.Role())
		assert.Equal(t, "", state.Leader())
		assert.False(t, state.HasVIP())
	})
}

func TestStateTransitions(t *testing.T) {
	t.Run("transition to primary when becoming leader", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		err := state.BecomeLeader()
		assert.NoError(t, err)
		assert.Equal(t, RolePrimary, state.Role())
		assert.Equal(t, "node-1", state.Leader())
	})

	t.Run("transition to passive when stepping down", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		_ = state.BecomeLeader()

		err := state.StepDown()
		assert.NoError(t, err)
		assert.Equal(t, RolePassive, state.Role())
		assert.Equal(t, "", state.Leader())
	})

	t.Run("update leader when another node becomes leader", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		state.SetLeader("node-2")
		assert.Equal(t, RolePassive, state.Role())
		assert.Equal(t, "node-2", state.Leader())
	})

	t.Run("cannot become leader if already primary", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		_ = state.BecomeLeader()

		err := state.BecomeLeader()
		assert.Error(t, err)
		assert.Equal(t, ErrAlreadyLeader, err)
	})

	t.Run("cannot step down if not leader", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		err := state.StepDown()
		assert.Error(t, err)
		assert.Equal(t, ErrNotLeader, err)
	})

	t.Run("clear leader when leader node disappears", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		state.SetLeader("node-2")

		state.ClearLeader()
		assert.Equal(t, "", state.Leader())
		assert.Equal(t, RolePassive, state.Role())
	})
}

func TestVIPState(t *testing.T) {
	t.Run("acquire VIP when becoming primary", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		_ = state.BecomeLeader()

		state.SetVIPAcquired(true)
		assert.True(t, state.HasVIP())
	})

	t.Run("release VIP when stepping down", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		_ = state.BecomeLeader()
		state.SetVIPAcquired(true)

		_ = state.StepDown()
		state.SetVIPAcquired(false)
		assert.False(t, state.HasVIP())
	})
}

func TestStateCallbacks(t *testing.T) {
	t.Run("calls OnBecomeLeader callback", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		called := false
		state.OnBecomeLeader(func() {
			called = true
		})

		_ = state.BecomeLeader()
		assert.True(t, called)
	})

	t.Run("calls OnStepDown callback", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		_ = state.BecomeLeader()

		called := false
		state.OnStepDown(func() {
			called = true
		})

		_ = state.StepDown()
		assert.True(t, called)
	})

	t.Run("calls OnLeaderChange callback", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		var newLeader string
		state.OnLeaderChange(func(leader string) {
			newLeader = leader
		})

		state.SetLeader("node-2")
		assert.Equal(t, "node-2", newLeader)
	})
}

func TestStateConcurrency(t *testing.T) {
	t.Run("safe concurrent access", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		done := make(chan bool)

		// Concurrent reads
		go func() {
			for i := 0; i < 100; i++ {
				_ = state.Role()
				_ = state.Leader()
				_ = state.HasVIP()
			}
			done <- true
		}()

		// Concurrent writes
		go func() {
			for i := 0; i < 100; i++ {
				if i%2 == 0 {
					state.SetLeader("node-2")
				} else {
					state.ClearLeader()
				}
			}
			done <- true
		}()

		<-done
		<-done
	})
}

func TestIsLeader(t *testing.T) {
	t.Run("returns true when this node is leader", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		_ = state.BecomeLeader()

		assert.True(t, state.IsLeader())
	})

	t.Run("returns false when another node is leader", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")
		state.SetLeader("node-2")

		assert.False(t, state.IsLeader())
	})

	t.Run("returns false when no leader", func(t *testing.T) {
		state := NewState("node-1", "cluster-1")

		assert.False(t, state.IsLeader())
	})
}
