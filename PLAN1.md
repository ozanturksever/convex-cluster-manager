# Plan: convex-cluster-manager - Go Cluster Management Daemon (TDD)

## Overview

Build a standalone Go binary (`convex-cluster-manager`) that provides active-passive clustering for Convex backends using NATS for coordination and Litestream for SQLite WAL replication. Development follows TDD with real dependencies via testcontainers.

## Requirements

### Core Daemon
- Single binary that runs as a systemd service
- Configuration via JSON file (`/etc/convex/cluster.json`) and CLI flags
- Structured logging with configurable levels

### CLI Commands
- `init` - Initialize cluster configuration for a node
- `join` - Join existing cluster and bootstrap from snapshot
- `leave` - Gracefully leave cluster
- `status` - Show cluster status (role, nodes, replication lag)
- `promote` - Force this node to become leader
- `demote` - Release leadership (become passive)
- `daemon` - Run the cluster daemon (default mode)

### Leader Election (NATS KV)
- NATS KV store for distributed leader election
- Leader key with TTL (default 10s)
- Heartbeat renewal (default 3s)
- Revision-based fencing to prevent split-brain

### WAL Replication (Litestream Library)
- Litestream's Go library for WAL capture and replay
- Primary: Capture WAL changes, publish to NATS JetStream
- Passive: Consume WAL frames, apply to shadow database

### Snapshots (NATS Object Store)
- Periodic full database snapshots (default 1 hour)
- Store in NATS Object Store for new node bootstrap

### Virtual IP Management
- Acquire/release VIP on leadership changes
- Use iproute2 (`ip addr add/del`)
- Gratuitous ARP on VIP acquisition

### Backend Control (systemd)
- Start/stop Convex backend via `systemctl`
- Health check via HTTP endpoint

## Project Structure

```
convex-cluster-manager/
├── cmd/
│   └── convex-cluster-manager/
│       └── main.go
├── internal/
│   ├── config/
│   │   ├── config.go
│   │   └── config_test.go
│   ├── cluster/
│   │   ├── daemon.go
│   │   ├── daemon_test.go
│   │   ├── election.go
│   │   ├── election_test.go
│   │   ├── state.go
│   │   └── state_test.go
│   ├── replication/
│   │   ├── primary.go
│   │   ├── primary_test.go
│   │   ├── passive.go
│   │   ├── passive_test.go
│   │   ├── snapshot.go
│   │   └── snapshot_test.go
│   ├── vip/
│   │   ├── manager.go
│   │   └── manager_test.go
│   ├── health/
│   │   ├── checker.go
│   │   └── checker_test.go
│   └── backend/
│       ├── controller.go
│       └── controller_test.go
├── testutil/
│   ├── containers.go       # Testcontainers setup
│   ├── systemd_image.go    # Systemd Docker image builder
│   └── helpers.go
├── docker/
│   └── Dockerfile.systemd  # Systemd-enabled test image
├── go.mod
└── README.md
```

## Testing Strategy (No Mocks - Real Dependencies)

### Testcontainers Setup

**NATS Container:**
- Use `nats:latest` image with JetStream enabled (`-js` flag)
- For all election, replication, snapshot, and health tests

**Systemd Container (for VIP + Backend tests):**
- Custom Ubuntu 24.04 image with systemd as PID 1
- Required Docker run flags:
  - `--privileged`
  - `--cgroupns=host`
  - `--tmpfs /run`, `--tmpfs /run/lock`
  - `-v /sys/fs/cgroup:/sys/fs/cgroup:rw`
- Install: `systemd`, `systemd-sysv`, `iproute2`, `iputils-ping`, `arping`
- Tests execute commands via `docker exec` to test real `systemctl` and `ip addr` commands

**SQLite:**
- Real SQLite databases using temp directories (no container needed)

### TDD Implementation Order

1. `config` - Configuration parsing (unit tests, no containers)
2. `cluster/state` - State machine transitions (unit tests)
3. `cluster/election` - Leader election (NATS testcontainer)
4. `vip/manager` - VIP management (systemd testcontainer with privileged mode)
5. `backend/controller` - systemctl wrapper (systemd testcontainer)
6. `replication/primary` - WAL publishing (NATS + SQLite)
7. `replication/passive` - WAL consumption (NATS + SQLite)
8. `replication/snapshot` - Snapshots (NATS Object Store + SQLite)
9. `health/checker` - Health checks (NATS testcontainer)
10. `cluster/daemon` - Full integration (NATS + systemd containers, two-node cluster)

### Systemd Test Container Image

Based on `docker-systemd.md`, the test image will:
- Use Ubuntu 24.04 as base
- Install systemd, iproute2, arping for VIP tests
- Remove unnecessary systemd services for faster boot
- Run `/lib/systemd/systemd` as CMD
- Wait for `systemctl is-system-running --wait` before tests

## Dependencies

- `github.com/nats-io/nats.go` - NATS client
- `github.com/benbjohnson/litestream` - WAL replication
- `github.com/spf13/cobra` - CLI framework
- `github.com/testcontainers/testcontainers-go` - Testcontainers
- `github.com/stretchr/testify` - Test assertions
- `github.com/prometheus/client_golang` - Metrics

## Relevant Files

- `litestream/nats/replica_client.go` - NATS Object Store integration
- `litestream/store.go` - Store for managing databases
- `litestream/db.go` - Database wrapper with WAL monitoring
- `litestream/_examples/library/basic/main.go` - Library usage example
- `docker-systemd.md` - Systemd container configuration guide

## Configuration Schema

```json
{
  "clusterId": "string (required)",
  "nodeId": "string (required)",
  "nats": {
    "servers": ["nats://host:port"],
    "credentials": "/path/to/creds (optional)"
  },
  "vip": {
    "address": "192.168.1.100",
    "netmask": 24,
    "interface": "eth0"
  },
  "election": {
    "leaderTtlMs": 10000,
    "heartbeatIntervalMs": 3000
  },
  "wal": {
    "streamRetention": "24h",
    "snapshotIntervalMs": 3600000,
    "replicaPath": "/var/lib/convex/replica"
  },
  "backend": {
    "serviceName": "convex-backend",
    "healthEndpoint": "http://localhost:3210/version",
    "dataPath": "/var/lib/convex/data"
  }
}
```

## Notes

- **No mocks** - All tests use real dependencies via testcontainers
- Systemd container tests require Docker daemon with privileged container support
- Tests may be skipped in CI environments without Docker privileged mode
- Use `testcontainers-go` for container lifecycle management
- Wait for systemd initialization (`systemctl is-system-running --wait`) before running commands
