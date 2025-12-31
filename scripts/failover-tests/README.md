# Cluster Failover Test Scripts

This directory contains ad-hoc test scripts for testing various cluster failover scenarios in the convex-cluster-manager.

## Prerequisites

Before running any failover test, you need to have a running cluster. The easiest way is to run the main e2e test with the `--keep` flag:

```bash
# Set up a test cluster and keep it running
# From the convex-cluster-manager directory:
./scripts/test-cluster-e2e.sh --keep
```

This will:
1. Build all necessary binaries (convex-bundler, convex-backend-ops, convex-cluster-manager)
2. Create a test bundle with a real Convex app
3. Start Docker containers for NATS and two cluster nodes
4. Initialize and start the cluster daemons
5. Leave everything running for further testing

## Test Scripts

### 1. Basic Failover Test (`test-cluster-failover-basic.sh`)

Tests the fundamental failover scenario: killing the primary daemon and verifying the passive node takes over.

```bash
./scripts/test-cluster-failover-basic.sh

# With custom container names
./scripts/test-cluster-failover-basic.sh \
    --node1-container my-node1 \
    --node2-container my-node2 \
    --nats-container my-nats
```

**What it tests:**
- Primary detection
- Daemon termination triggers failover
- Passive node promotion to PRIMARY
- Backend availability on new primary
- Former primary rejoining as PASSIVE

### 2. Graceful Failover Test (`test-cluster-failover-graceful.sh`)

Tests graceful leadership transfer using the `convex-cluster-manager demote` command.

```bash
./scripts/test-cluster-failover-graceful.sh
```

**What it tests:**
- Graceful stepdown via `demote` command
- Leadership transfer without service interruption
- Failback (second demote to return leadership)
- Cluster stability after multiple graceful transfers

### 3. Rapid Failover Test (`test-cluster-failover-rapid.sh`)

Stress tests the cluster with multiple consecutive failovers.

```bash
# Default: 3 iterations, 20s wait between failovers
./scripts/test-cluster-failover-rapid.sh

# Custom iterations and timing
./scripts/test-cluster-failover-rapid.sh --iterations 5 --wait-time 15
```

**What it tests:**
- Cluster stability under repeated failovers
- Leader election convergence time
- Daemon restart reliability
- Success rate across multiple failover cycles

### 4. Data Integrity Failover Test (`test-cluster-failover-data-integrity.sh`)

Verifies that data is preserved and replicated correctly across failovers.

```bash
./scripts/test-cluster-failover-data-integrity.sh

# With explicit admin key
./scripts/test-cluster-failover-data-integrity.sh --admin-key "your-admin-key"
```

**What it tests:**
- Data inserted before failover is preserved
- Data inserted after failover is preserved
- Data survives multiple failover cycles
- WAL replication to passive nodes

### 5. Network Partition Test (`test-cluster-failover-network.sh`)

Simulates network partitions by disconnecting containers from the Docker network.

```bash
./scripts/test-cluster-failover-network.sh

# Custom partition duration
./scripts/test-cluster-failover-network.sh --partition-time 45
```

**What it tests:**
- Failover on network partition
- Split-brain prevention (only one PRIMARY)
- Cluster recovery after network heals
- Leader election after reconnection

## Common Options

All scripts support these options:

| Option | Description | Default |
|--------|-------------|---------|
| `--nats-container NAME` | NATS container name | `convex-e2e-nats` |
| `--node1-container NAME` | Node 1 container name | `convex-e2e-node1` |
| `--node2-container NAME` | Node 2 container name | `convex-e2e-node2` |
| `--verbose` or `-v` | Show verbose output | `false` |
| `--help` or `-h` | Show help message | - |

## Shared Library

The `lib-failover-common.sh` script provides common functions used by all tests:

- `get_primary_node` / `get_passive_node` - Detect current cluster roles
- `wait_for_primary` / `wait_for_passive` - Wait for role transitions
- `kill_daemon` / `start_daemon` - Control cluster daemons
- `get_node_status` / `get_node_role` - Query node status
- `call_mutation` / `call_query` - Make Convex API calls
- `wait_for_backend` / `check_backend_health` - Backend health checks
- `print_cluster_status` - Display formatted cluster status
- `disconnect_from_network` / `reconnect_to_network` - Network manipulation

## Example Workflow

```bash
# From the convex-cluster-manager directory:

# 1. Start the test cluster
./scripts/test-cluster-e2e.sh --keep

# 2. Run basic failover test
./scripts/test-cluster-failover-basic.sh

# 3. Run graceful failover test
./scripts/test-cluster-failover-graceful.sh

# 4. Run rapid failover stress test
./scripts/test-cluster-failover-rapid.sh --iterations 5

# 5. Run data integrity test
./scripts/test-cluster-failover-data-integrity.sh

# 6. Run network partition test
./scripts/test-cluster-failover-network.sh

# 7. Clean up when done
docker rm -f convex-e2e-nats convex-e2e-node1 convex-e2e-node2
docker network rm convex-e2e-network
rm -rf test-cluster-e2e-output
```

## Debugging Failed Tests

### Check container logs

```bash
# Cluster manager daemon logs
docker exec convex-e2e-node1 cat /var/log/cluster-manager.log
docker exec convex-e2e-node2 cat /var/log/cluster-manager.log

# Backend logs (if using systemd)
docker exec convex-e2e-node1 journalctl -u convex-backend --no-pager -n 50
```

### Check cluster status manually

```bash
# Node 1 status
docker exec convex-e2e-node1 /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json

# Node 2 status
docker exec convex-e2e-node2 /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json
```

### Interactive debugging

```bash
# Shell into a container
docker exec -it convex-e2e-node1 bash

# Check daemon process
ps aux | grep convex-cluster-manager

# Check backend process
ps aux | grep convex-backend

# Check database WAL mode
sqlite3 /var/lib/convex/data/convex.db "PRAGMA journal_mode;"
```

## Test Environment Variables

You can set these environment variables to customize test behavior:

```bash
export PROJECT_ROOT=/path/to/convex
export BUNDLER_DIR=/path/to/convex-bundler
export CLUSTER_MGR_DIR=/path/to/convex-cluster-manager
export TEST_APP_DIR=/path/to/test-app
```

## Writing New Tests

To create a new failover test:

1. Create a new script in `scripts/`
2. Source the common library:
   ```bash
   source "$SCRIPT_DIR/failover-tests/lib-failover-common.sh"
   ```
3. Use the provided helper functions
4. Follow the existing test structure (steps, validation, summary)
5. Use `print_test_result` to report pass/fail status

## Troubleshooting

### "No PRIMARY node detected"

The cluster daemons may not be running. Try:
```bash
docker exec convex-e2e-node1 /usr/local/bin/convex-cluster-manager daemon --config /etc/convex/cluster.json &
docker exec convex-e2e-node2 /usr/local/bin/convex-cluster-manager daemon --config /etc/convex/cluster.json &
```

### "Backend not healthy"

The Convex backend may not be running. Check:
```bash
docker exec convex-e2e-node1 systemctl status convex-backend
docker exec convex-e2e-node1 curl http://localhost:3210/version
```

### Network partition test fails to disconnect

Ensure you're using the correct network name:
```bash
docker network ls
```

### Tests timeout waiting for failover

The leader TTL is typically 10 seconds. If tests timeout:
- Check NATS connectivity
- Verify cluster configuration (`/etc/convex/cluster.json`)
- Look for errors in daemon logs
