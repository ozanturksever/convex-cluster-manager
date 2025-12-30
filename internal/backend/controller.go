// Package backend provides backend service management via systemd.
package backend

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// ServiceStatus represents the status of a systemd service.
type ServiceStatus int

const (
	// StatusUnknown indicates the service status is unknown.
	StatusUnknown ServiceStatus = iota
	// StatusRunning indicates the service is running.
	StatusRunning
	// StatusStopped indicates the service is stopped.
	StatusStopped
	// StatusFailed indicates the service has failed.
	StatusFailed
)

// String returns the string representation of the service status.
func (s ServiceStatus) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusStopped:
		return "stopped"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Config contains backend controller configuration.
type Config struct {
	ServiceName    string
	HealthEndpoint string
	DataPath       string
}

// Validate validates the backend configuration.
func (c Config) Validate() error {
	if c.ServiceName == "" {
		return fmt.Errorf("serviceName is required")
	}
	return nil
}

// Executor executes system commands.
type Executor interface {
	Execute(ctx context.Context, cmd string, args ...string) (string, error)
}

// Controller manages a backend service via systemd.
type Controller struct {
	cfg      Config
	executor Executor
	logger   *slog.Logger
}

// NewController creates a new backend controller.
func NewController(cfg Config, executor Executor) *Controller {
	return &Controller{
		cfg:      cfg,
		executor: executor,
		logger:   slog.Default().With("component", "backend", "service", cfg.ServiceName),
	}
}

// Start starts the backend service.
func (c *Controller) Start(ctx context.Context) error {
	_, err := c.executor.Execute(ctx, "systemctl", "start", c.cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("failed to start service %s: %w", c.cfg.ServiceName, err)
	}
	c.logger.Info("service started")
	return nil
}

// Stop stops the backend service.
func (c *Controller) Stop(ctx context.Context) error {
	_, err := c.executor.Execute(ctx, "systemctl", "stop", c.cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("failed to stop service %s: %w", c.cfg.ServiceName, err)
	}
	c.logger.Info("service stopped")
	return nil
}

// Restart restarts the backend service.
func (c *Controller) Restart(ctx context.Context) error {
	_, err := c.executor.Execute(ctx, "systemctl", "restart", c.cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("failed to restart service %s: %w", c.cfg.ServiceName, err)
	}
	c.logger.Info("service restarted")
	return nil
}

// Status returns the current status of the backend service.
func (c *Controller) Status(ctx context.Context) (ServiceStatus, error) {
	output, err := c.executor.Execute(ctx, "systemctl", "is-active", c.cfg.ServiceName)
	
	// systemctl is-active returns non-zero for inactive services
	// so we need to check the output regardless of error
	output = strings.TrimSpace(output)
	
	switch output {
	case "active":
		return StatusRunning, nil
	case "inactive", "deactivating":
		return StatusStopped, nil
	case "failed":
		return StatusFailed, nil
	default:
		// If we got an error and no recognizable output, return stopped
		if err != nil {
			return StatusStopped, nil
		}
		return StatusUnknown, nil
	}
}

// Enable enables the backend service to start at boot.
func (c *Controller) Enable(ctx context.Context) error {
	_, err := c.executor.Execute(ctx, "systemctl", "enable", c.cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("failed to enable service %s: %w", c.cfg.ServiceName, err)
	}
	c.logger.Info("service enabled")
	return nil
}

// Disable disables the backend service from starting at boot.
func (c *Controller) Disable(ctx context.Context) error {
	_, err := c.executor.Execute(ctx, "systemctl", "disable", c.cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("failed to disable service %s: %w", c.cfg.ServiceName, err)
	}
	c.logger.Info("service disabled")
	return nil
}

// IsEnabled returns whether the service is enabled to start at boot.
func (c *Controller) IsEnabled(ctx context.Context) (bool, error) {
	output, err := c.executor.Execute(ctx, "systemctl", "is-enabled", c.cfg.ServiceName)
	
	// systemctl is-enabled returns non-zero for disabled services
	output = strings.TrimSpace(output)
	
	switch output {
	case "enabled", "enabled-runtime":
		return true, nil
	case "disabled", "masked", "static":
		return false, nil
	default:
		if err != nil {
			return false, nil
		}
		return false, nil
	}
}
