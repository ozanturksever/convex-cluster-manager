# Convex Cluster Manager

Active-passive clustering for Convex backends using NATS for coordination and Litestream for SQLite WAL replication.

## Features

- **Leader Election**: NATS KV-based distributed leader election with TTL and heartbeat renewal
- **WAL Replication**: Litestream-powered WAL capture and replay via NATS JetStream
- **Snapshots**: Periodic full database snapshots stored in NATS Object Store
- **Virtual IP Management**: Automatic VIP failover on leadership changes
- **Backend Control**: systemd-based Convex backend service management
- **Health Checks**: HTTP-based health monitoring

## Installation

### From Binary Releases

Download the latest release from the [releases page](https://github.com/ozanturksever/convex-cluster-manager/releases).

```bash
# Linux (amd64) - replace VERSION with the latest release version (e.g., 0.1.0)
curl -LO https://github.com/ozanturksever/convex-cluster-manager/releases/latest/download/convex-cluster-manager_VERSION_linux_amd64.tar.gz
tar xzf convex-cluster-manager_VERSION_linux_amd64.tar.gz
sudo mv convex-cluster-manager /usr/local/bin/

# macOS (arm64 - Apple Silicon)
curl -LO https://github.com/ozanturksever/convex-cluster-manager/releases/latest/download/convex-cluster-manager_VERSION_darwin_arm64.tar.gz
tar xzf convex-cluster-manager_VERSION_darwin_arm64.tar.gz
sudo mv convex-cluster-manager /usr/local/bin/
```

### From Source

```bash
go install github.com/ozanturksever/convex-cluster-manager/cmd/convex-cluster-manager@latest
```

### Using Go

```bash
git clone https://github.com/ozanturksever/convex-cluster-manager.git
cd convex-cluster-manager
go build -o convex-cluster-manager ./cmd/convex-cluster-manager
```

## Quick Start

### 1. Initialize Configuration

```bash
convex-cluster-manager init \
  --cluster-id my-cluster \
  --node-id node1 \
  --nats nats://localhost:4222 \
  --vip 192.168.1.100 \
  --interface eth0
```

### 2. Start the Daemon

```bash
convex-cluster-manager daemon
```

### 3. Check Cluster Status

```bash
convex-cluster-manager status
```

## Commands

| Command   | Description                                      |
|-----------|--------------------------------------------------|
| `init`    | Initialize cluster configuration for this node  |
| `join`    | Join existing cluster and bootstrap from snapshot|
| `leave`   | Gracefully leave the cluster                    |
| `status`  | Show cluster status (role, leader, VIP)         |
| `promote` | Force this node to become leader                |
| `demote`  | Release leadership and become passive           |
| `daemon`  | Run the cluster daemon                          |
| `version` | Show version information                        |

## Configuration

Configuration file location: `/etc/convex/cluster.json`

```json
{
  "clusterId": "my-cluster",
  "nodeId": "node1",
  "nats": {
    "servers": ["nats://localhost:4222"]
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

## Architecture

The cluster manager implements an active-passive clustering pattern:

1. **Primary Node**: Handles all traffic, captures WAL changes, publishes to NATS JetStream
2. **Passive Nodes**: Consume WAL frames, maintain shadow database, ready for failover
3. **NATS**: Provides coordination (KV for election), messaging (JetStream for WAL), and storage (Object Store for snapshots)

## Requirements

- NATS server with JetStream enabled
- Linux (for VIP management via iproute2)
- systemd (for backend service control)

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
