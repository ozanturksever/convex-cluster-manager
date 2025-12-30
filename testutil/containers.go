// Package testutil provides test utilities including container setup for integration tests.
package testutil

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NATSContainer represents a running NATS container with JetStream enabled.
type NATSContainer struct {
	Container testcontainers.Container
	URL       string
}

// StartNATSContainer starts a NATS container with JetStream enabled.
func StartNATSContainer(ctx context.Context) (*NATSContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "nats:latest",
		ExposedPorts: []string{"4222/tcp"},
		Cmd:          []string{"-js", "-sd", "/data"},
		WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start NATS container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get NATS container host: %w", err)
	}

	port, err := container.MappedPort(ctx, "4222")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get NATS container port: %w", err)
	}

	return &NATSContainer{
		Container: container,
		URL:       fmt.Sprintf("nats://%s:%s", host, port.Port()),
	}, nil
}

// Stop terminates the NATS container.
func (n *NATSContainer) Stop(ctx context.Context) error {
	if n.Container != nil {
		return n.Container.Terminate(ctx)
	}
	return nil
}

// SystemdContainer represents a running systemd-enabled container.
type SystemdContainer struct {
	Container testcontainers.Container
}

// StartSystemdContainer starts a systemd-enabled container for testing VIP and backend control.
// The container runs with privileged mode and proper cgroup configuration.
func StartSystemdContainer(ctx context.Context, dockerfilePath string) (*SystemdContainer, error) {
	// Build context is the directory containing the Dockerfile
	buildContext := filepath.Dir(dockerfilePath)
	dockerfileName := filepath.Base(dockerfilePath)

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    buildContext,
			Dockerfile: dockerfileName,
		},
		Privileged: true,
		Tmpfs: map[string]string{
			"/run":      "rw,noexec,nosuid",
			"/run/lock": "rw,noexec,nosuid",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.CgroupnsMode = "host"
			hc.Binds = append(hc.Binds, "/sys/fs/cgroup:/sys/fs/cgroup:rw")
		},
		WaitingFor: wait.ForExec([]string{"systemctl", "is-system-running", "--wait"}).
			WithStartupTimeout(60 * time.Second).
			WithPollInterval(1 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start systemd container: %w", err)
	}

	return &SystemdContainer{
		Container: container,
	}, nil
}

// Exec executes a command in the systemd container and returns the output.
func (s *SystemdContainer) Exec(ctx context.Context, cmd []string) (int, string, error) {
	exitCode, reader, err := s.Container.Exec(ctx, cmd)
	if err != nil {
		return exitCode, "", fmt.Errorf("failed to execute command: %w", err)
	}

	// Read all output
	output, err := io.ReadAll(reader)
	if err != nil {
		return exitCode, "", fmt.Errorf("failed to read command output: %w", err)
	}

	return exitCode, string(output), nil
}

// Stop terminates the systemd container.
func (s *SystemdContainer) Stop(ctx context.Context) error {
	if s.Container != nil {
		return s.Container.Terminate(ctx)
	}
	return nil
}
