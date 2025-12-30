// Package vip provides Virtual IP management functionality.
package vip

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
)

// Config contains VIP configuration.
type Config struct {
	Address   string
	Netmask   int
	Interface string
}

// CIDR returns the VIP address in CIDR notation.
func (c Config) CIDR() string {
	return fmt.Sprintf("%s/%d", c.Address, c.Netmask)
}

// Validate validates the VIP configuration.
func (c Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("address is required")
	}
	if net.ParseIP(c.Address) == nil {
		return fmt.Errorf("invalid IP address: %s", c.Address)
	}
	if c.Netmask < 0 || c.Netmask > 32 {
		return fmt.Errorf("netmask must be between 0 and 32")
	}
	if c.Interface == "" {
		return fmt.Errorf("interface is required")
	}
	return nil
}

// Executor executes system commands.
type Executor interface {
	Execute(ctx context.Context, cmd string, args ...string) (string, error)
}

// Manager manages a Virtual IP address.
type Manager struct {
	cfg      Config
	executor Executor
	logger   *slog.Logger

	mu       sync.Mutex
	acquired bool
}

// NewManager creates a new VIP manager.
func NewManager(cfg Config, executor Executor) *Manager {
	return &Manager{
		cfg:      cfg,
		executor: executor,
		logger:   slog.Default().With("component", "vip", "address", cfg.Address),
	}
}

// Acquire adds the VIP to the configured interface.
func (m *Manager) Acquire(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already acquired
	acquired, err := m.isAcquiredLocked(ctx)
	if err != nil {
		return fmt.Errorf("failed to check VIP status: %w", err)
	}
	if acquired {
		m.acquired = true
		return nil
	}

	// Add the IP address
	cidr := m.cfg.CIDR()
	_, err = m.executor.Execute(ctx, "ip", "addr", "add", cidr, "dev", m.cfg.Interface)
	if err != nil {
		// Check if it's already added (RTNETLINK answers: File exists)
		if strings.Contains(err.Error(), "File exists") {
			m.acquired = true
			return nil
		}
		return fmt.Errorf("failed to add VIP %s: %w", cidr, err)
	}

	m.acquired = true
	m.logger.Info("VIP acquired", "interface", m.cfg.Interface)

	// Send gratuitous ARP (best effort, ignore errors)
	m.sendGratuitousARP(ctx)

	return nil
}

// Release removes the VIP from the configured interface.
func (m *Manager) Release(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if currently acquired
	acquired, err := m.isAcquiredLocked(ctx)
	if err != nil {
		return fmt.Errorf("failed to check VIP status: %w", err)
	}
	if !acquired {
		m.acquired = false
		return nil
	}

	// Remove the IP address
	cidr := m.cfg.CIDR()
	_, err = m.executor.Execute(ctx, "ip", "addr", "del", cidr, "dev", m.cfg.Interface)
	if err != nil {
		// Check if it's already removed
		if strings.Contains(err.Error(), "Cannot assign requested address") {
			m.acquired = false
			return nil
		}
		return fmt.Errorf("failed to remove VIP %s: %w", cidr, err)
	}

	m.acquired = false
	m.logger.Info("VIP released", "interface", m.cfg.Interface)

	return nil
}

// IsAcquired checks if the VIP is currently assigned to the interface.
func (m *Manager) IsAcquired(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isAcquiredLocked(ctx)
}

func (m *Manager) isAcquiredLocked(ctx context.Context) (bool, error) {
	output, err := m.executor.Execute(ctx, "ip", "addr", "show", "dev", m.cfg.Interface)
	if err != nil {
		return false, fmt.Errorf("failed to check interface: %w", err)
	}

	// Check if our IP is in the output
	return strings.Contains(output, m.cfg.Address), nil
}

func (m *Manager) sendGratuitousARP(ctx context.Context) {
	// Try arping first (more common)
	_, err := m.executor.Execute(ctx, "arping", "-c", "3", "-A", "-I", m.cfg.Interface, m.cfg.Address)
	if err != nil {
		m.logger.Debug("arping failed (may not be installed)", "error", err)
	}
}
