package vip

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"
)

func TestVIPManager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start systemd container
	systemdContainer, err := testutil.StartSystemdContainer(ctx, "../../../docker/Dockerfile.systemd")
	if err != nil {
		t.Skipf("skipping test: failed to start systemd container: %v", err)
	}
	defer systemdContainer.Stop(ctx)

	// Wait for systemd to be ready
	time.Sleep(3 * time.Second)

	t.Run("acquire VIP", func(t *testing.T) {
		cfg := Config{
			Address:   "10.200.200.100",
			Netmask:   24,
			Interface: "lo", // Use loopback for testing
		}

		manager := NewManager(cfg, &containerExecutor{container: systemdContainer})

		err := manager.Acquire(ctx)
		require.NoError(t, err)

		// Verify VIP was added
		acquired, err := manager.IsAcquired(ctx)
		require.NoError(t, err)
		assert.True(t, acquired)

		// Clean up
		err = manager.Release(ctx)
		require.NoError(t, err)
	})

	t.Run("release VIP", func(t *testing.T) {
		cfg := Config{
			Address:   "10.200.200.101",
			Netmask:   24,
			Interface: "lo",
		}

		manager := NewManager(cfg, &containerExecutor{container: systemdContainer})

		// First acquire
		err := manager.Acquire(ctx)
		require.NoError(t, err)

		// Then release
		err = manager.Release(ctx)
		require.NoError(t, err)

		// Verify VIP was removed
		acquired, err := manager.IsAcquired(ctx)
		require.NoError(t, err)
		assert.False(t, acquired)
	})

	t.Run("idempotent acquire", func(t *testing.T) {
		cfg := Config{
			Address:   "10.200.200.102",
			Netmask:   24,
			Interface: "lo",
		}

		manager := NewManager(cfg, &containerExecutor{container: systemdContainer})

		// Acquire twice should not error
		err := manager.Acquire(ctx)
		require.NoError(t, err)

		err = manager.Acquire(ctx)
		require.NoError(t, err)

		// Clean up
		err = manager.Release(ctx)
		require.NoError(t, err)
	})

	t.Run("idempotent release", func(t *testing.T) {
		cfg := Config{
			Address:   "10.200.200.103",
			Netmask:   24,
			Interface: "lo",
		}

		manager := NewManager(cfg, &containerExecutor{container: systemdContainer})

		// Release without acquire should not error
		err := manager.Release(ctx)
		require.NoError(t, err)
	})
}

func TestVIPConfig(t *testing.T) {
	t.Run("CIDR returns correct format", func(t *testing.T) {
		cfg := Config{
			Address:   "192.168.1.100",
			Netmask:   24,
			Interface: "eth0",
		}
		assert.Equal(t, "192.168.1.100/24", cfg.CIDR())
	})

	t.Run("validates required fields", func(t *testing.T) {
		cfg := Config{}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("validates IP address format", func(t *testing.T) {
		cfg := Config{
			Address:   "invalid",
			Netmask:   24,
			Interface: "eth0",
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("validates netmask range", func(t *testing.T) {
		cfg := Config{
			Address:   "192.168.1.100",
			Netmask:   33,
			Interface: "eth0",
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("passes with valid config", func(t *testing.T) {
		cfg := Config{
			Address:   "192.168.1.100",
			Netmask:   24,
			Interface: "eth0",
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})
}

// containerExecutor executes commands in a testcontainer
type containerExecutor struct {
	container *testutil.SystemdContainer
}

func (e *containerExecutor) Execute(ctx context.Context, cmd string, args ...string) (string, error) {
	fullCmd := append([]string{cmd}, args...)
	_, output, err := e.container.Exec(ctx, fullCmd)
	return strings.TrimSpace(output), err
}
