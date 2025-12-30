package backend

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozanturksever/convex-cluster-manager/testutil"
)

func TestController(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start systemd container
	systemdContainer, err := testutil.StartSystemdContainer(ctx, "../../../docker/Dockerfile.systemd")
	if err != nil {
		t.Skipf("skipping test: failed to start systemd container: %v", err)
	}
	defer func() { _ = systemdContainer.Stop(ctx) }()

	// Wait for systemd to be ready
	time.Sleep(3 * time.Second)

	t.Run("start service", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-backend",
		}

		controller := NewController(cfg, &containerExecutor{container: systemdContainer})

		err := controller.Start(ctx)
		require.NoError(t, err)

		// Verify service is running
		status, err := controller.Status(ctx)
		require.NoError(t, err)
		assert.Equal(t, StatusRunning, status)

		// Clean up
		err = controller.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("stop service", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-backend",
		}

		controller := NewController(cfg, &containerExecutor{container: systemdContainer})

		// First start
		err := controller.Start(ctx)
		require.NoError(t, err)

		// Then stop
		err = controller.Stop(ctx)
		require.NoError(t, err)

		// Verify service is stopped
		status, err := controller.Status(ctx)
		require.NoError(t, err)
		assert.Equal(t, StatusStopped, status)
	})

	t.Run("status of non-running service", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-backend",
		}

		controller := NewController(cfg, &containerExecutor{container: systemdContainer})

		// Make sure it's stopped first
		_ = controller.Stop(ctx)

		status, err := controller.Status(ctx)
		require.NoError(t, err)
		assert.Equal(t, StatusStopped, status)
	})

	t.Run("enable service", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-backend",
		}

		controller := NewController(cfg, &containerExecutor{container: systemdContainer})

		err := controller.Enable(ctx)
		require.NoError(t, err)

		enabled, err := controller.IsEnabled(ctx)
		require.NoError(t, err)
		assert.True(t, enabled)

		// Clean up
		err = controller.Disable(ctx)
		require.NoError(t, err)
	})

	t.Run("disable service", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-backend",
		}

		controller := NewController(cfg, &containerExecutor{container: systemdContainer})

		// First enable
		err := controller.Enable(ctx)
		require.NoError(t, err)

		// Then disable
		err = controller.Disable(ctx)
		require.NoError(t, err)

		enabled, err := controller.IsEnabled(ctx)
		require.NoError(t, err)
		assert.False(t, enabled)
	})
}

func TestControllerConfig(t *testing.T) {
	t.Run("validates service name required", func(t *testing.T) {
		cfg := Config{}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("passes with valid config", func(t *testing.T) {
		cfg := Config{
			ServiceName: "convex-backend",
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})
}

func TestServiceStatus(t *testing.T) {
	t.Run("string representation", func(t *testing.T) {
		assert.Equal(t, "running", StatusRunning.String())
		assert.Equal(t, "stopped", StatusStopped.String())
		assert.Equal(t, "failed", StatusFailed.String())
		assert.Equal(t, "unknown", ServiceStatus(99).String())
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
