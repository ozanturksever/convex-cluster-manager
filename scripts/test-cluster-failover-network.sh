#!/bin/bash
# test-cluster-failover-network.sh
# Network partition failover test: Simulate network partition and verify failover
#
# Prerequisites:
#   - Run scripts/test-cluster-e2e.sh --keep first to set up the cluster
#   - Or have a running cluster with the expected container names
#
# Usage:
#   ./test-cluster-failover-network.sh [OPTIONS]
#
# Options:
#   --nats-container NAME    NATS container name (default: convex-e2e-nats)
#   --node1-container NAME   Node1 container name (default: convex-e2e-node1)
#   --node2-container NAME   Node2 container name (default: convex-e2e-node2)
#   --network NAME           Docker network name (default: convex-e2e-network)
#   --partition-time SECS    How long to partition the network (default: 30)
#   --verbose                Show verbose output
#   --help                   Show help message

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CLUSTER_MGR_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
export PROJECT_ROOT="$(cd "$CLUSTER_MGR_DIR/.." && pwd)"

# Source common library
source "$SCRIPT_DIR/failover-tests/lib-failover-common.sh"

# Default configuration
NATS_CONTAINER="convex-e2e-nats"
NODE1_CONTAINER="convex-e2e-node1"
NODE2_CONTAINER="convex-e2e-node2"
NETWORK_NAME="convex-e2e-network"
PARTITION_TIME=30
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --nats-container)
            NATS_CONTAINER="$2"
            shift 2
            ;;
        --node1-container)
            NODE1_CONTAINER="$2"
            shift 2
            ;;
        --node2-container)
            NODE2_CONTAINER="$2"
            shift 2
            ;;
        --network)
            NETWORK_NAME="$2"
            shift 2
            ;;
        --partition-time)
            PARTITION_TIME="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Network partition failover test: Simulate network partition"
            echo ""
            echo "Options:"
            echo "  --nats-container NAME    NATS container name (default: convex-e2e-nats)"
            echo "  --node1-container NAME   Node1 container name (default: convex-e2e-node1)"
            echo "  --node2-container NAME   Node2 container name (default: convex-e2e-node2)"
            echo "  --network NAME           Docker network name (default: convex-e2e-network)"
            echo "  --partition-time SECS    How long to partition (default: 30)"
            echo "  --verbose                Show verbose output"
            echo "  --help                   Show help message"
            echo ""
            echo "Prerequisites:"
            echo "  Run 'scripts/test-cluster-e2e.sh --keep' first to set up the cluster"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Test variables
TEST_NAME="Network Partition Failover Test"
TEST_PASSED=true

# Ensure we restore network connectivity on exit
cleanup() {
    echo ""
    log_warn "Cleaning up - ensuring network connectivity is restored..."
    docker network connect "$NETWORK_NAME" "$NODE1_CONTAINER" 2>/dev/null || true
    docker network connect "$NETWORK_NAME" "$NODE2_CONTAINER" 2>/dev/null || true
}

trap cleanup EXIT

print_test_header "$TEST_NAME"

echo -e "${CYAN}Test Configuration:${NC}"
echo "  Network: $NETWORK_NAME"
echo "  Partition duration: ${PARTITION_TIME}s"
echo ""

# Step 1: Verify containers and network exist
log_step 1 "Verifying cluster containers and network..."

if ! check_required_containers "$NATS_CONTAINER" "$NODE1_CONTAINER" "$NODE2_CONTAINER"; then
    log_error "Required containers not running. Run 'scripts/test-cluster-e2e.sh --keep' first."
    exit 1
fi

if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK_NAME}$"; then
    log_error "Network '$NETWORK_NAME' does not exist"
    exit 1
fi

log_success "All containers and network exist"

# Step 2: Ensure both daemons are running
log_step 2 "Ensuring cluster is stable..."

start_daemon "$NODE1_CONTAINER" 2>/dev/null || true
start_daemon "$NODE2_CONTAINER" 2>/dev/null || true

# Wait for leader election with retries (similar to other tests)
log_info "Waiting for leader election..."
MAX_RETRIES=6
RETRY_DELAY=10
PRIMARY_NODE=""
PASSIVE_NODE=""

for retry in $(seq 1 $MAX_RETRIES); do
    sleep $RETRY_DELAY
    PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    
    if [ -n "$PRIMARY_NODE" ] && [ -n "$PASSIVE_NODE" ]; then
        break
    fi
    
    log_info "Retry $retry/$MAX_RETRIES: Waiting for PRIMARY and PASSIVE..."
done

if [ -z "$PRIMARY_NODE" ] || [ -z "$PASSIVE_NODE" ]; then
    log_error "Cluster not in expected state after $MAX_RETRIES retries (need PRIMARY and PASSIVE)"
    log_info "PRIMARY_NODE: ${PRIMARY_NODE:-<empty>}"
    log_info "PASSIVE_NODE: ${PASSIVE_NODE:-<empty>}"
    print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"
    exit 1
fi

log_success "Cluster is stable"
log_info "PRIMARY: $PRIMARY_NODE"
log_info "PASSIVE: $PASSIVE_NODE"

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# Step 3: Simulate network partition on PRIMARY
log_step 3 "Simulating network partition on PRIMARY node..."

log_info "Disconnecting $PRIMARY_NODE from $NETWORK_NAME"
ORIGINAL_PRIMARY="$PRIMARY_NODE"
ORIGINAL_PASSIVE="$PASSIVE_NODE"

docker network disconnect "$NETWORK_NAME" "$PRIMARY_NODE" 2>/dev/null || {
    log_error "Failed to disconnect $PRIMARY_NODE from network"
    TEST_PASSED=false
}

log_success "Network partition created - $PRIMARY_NODE is isolated"

# Step 4: Wait for partition duration and leader TTL to expire
log_step 4 "Waiting for network partition effects (${PARTITION_TIME}s)..."

log_info "The passive node should detect leader loss and attempt to become primary"
log_info "Leader TTL should expire after ~10-15 seconds"

sleep $PARTITION_TIME

# Step 5: Check cluster state during partition
log_step 5 "Checking cluster state during partition..."

# The passive node should now be primary (or trying to become primary)
PASSIVE_ROLE_DURING=$(get_node_role "$PASSIVE_NODE")
log_info "Former PASSIVE ($PASSIVE_NODE) role during partition: $PASSIVE_ROLE_DURING"

# The partitioned node can't communicate with NATS, so its status may be stale
log_info "Partitioned node ($PRIMARY_NODE) is isolated and can't update status"

if [ "$PASSIVE_ROLE_DURING" = "PRIMARY" ]; then
    log_success "Failover occurred! Former passive is now PRIMARY"
else
    log_warn "Former passive is still: $PASSIVE_ROLE_DURING (may need more time)"
fi

# Step 6: Restore network connectivity
log_step 6 "Restoring network connectivity..."

log_info "Reconnecting $ORIGINAL_PRIMARY to $NETWORK_NAME"
docker network connect "$NETWORK_NAME" "$ORIGINAL_PRIMARY" 2>/dev/null || {
    log_warn "Failed to reconnect $ORIGINAL_PRIMARY (may already be connected)"
}

log_success "Network connectivity restored"

# Step 7: Wait for cluster to stabilize after healing
log_step 7 "Waiting for cluster to stabilize after network heal..."

log_info "Waiting 20 seconds for election convergence..."
sleep 20

# Step 8: Verify final cluster state
log_step 8 "Verifying final cluster state..."

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

FINAL_PRIMARY=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
FINAL_PASSIVE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

FINAL_NODE1_ROLE=$(get_node_role "$NODE1_CONTAINER")
FINAL_NODE2_ROLE=$(get_node_role "$NODE2_CONTAINER")

log_info "Final Node1 role: $FINAL_NODE1_ROLE"
log_info "Final Node2 role: $FINAL_NODE2_ROLE"

# Verify we have exactly one PRIMARY and one PASSIVE
if [ -n "$FINAL_PRIMARY" ] && [ -n "$FINAL_PASSIVE" ]; then
    log_success "Cluster recovered: One PRIMARY and one PASSIVE"
else
    log_error "Cluster in unexpected state after network heal"
    TEST_PASSED=false
fi

# Verify backends are healthy
log_info "Checking backend health..."

if [ -n "$FINAL_PRIMARY" ] && wait_for_backend "$FINAL_PRIMARY" 30; then
    log_success "Backend healthy on PRIMARY ($FINAL_PRIMARY)"
else
    log_error "Backend not healthy on PRIMARY"
    TEST_PASSED=false
fi

# Step 9: Test split-brain prevention
log_step 9 "Testing split-brain prevention..."

# Check that we don't have two primaries (split-brain)
if [ "$FINAL_NODE1_ROLE" = "PRIMARY" ] && [ "$FINAL_NODE2_ROLE" = "PRIMARY" ]; then
    log_error "SPLIT-BRAIN DETECTED! Both nodes claim to be PRIMARY"
    TEST_PASSED=false
else
    log_success "No split-brain: Only one PRIMARY exists"
fi

# Step 10: Verify which node ended up as PRIMARY
log_step 10 "Analyzing failover behavior..."

if [ "$FINAL_PRIMARY" = "$ORIGINAL_PASSIVE" ]; then
    log_success "Failover confirmed: Original passive ($ORIGINAL_PASSIVE) became PRIMARY"
    log_info "Original primary ($ORIGINAL_PRIMARY) should now be PASSIVE"
    
    if [ "$FINAL_PASSIVE" = "$ORIGINAL_PRIMARY" ]; then
        log_success "Correct: Original primary is now PASSIVE"
    else
        log_warn "Original primary has role: $(get_node_role "$ORIGINAL_PRIMARY")"
    fi
elif [ "$FINAL_PRIMARY" = "$ORIGINAL_PRIMARY" ]; then
    log_info "Original primary ($ORIGINAL_PRIMARY) reclaimed leadership after network heal"
    log_info "This can happen if:"
    log_info "  - Network healed before TTL expired"
    log_info "  - Original primary won re-election"
else
    log_warn "Unexpected primary: $FINAL_PRIMARY"
fi

# Print summary
echo ""
echo -e "${CYAN}━━━ Network Partition Test Summary ━━━${NC}"
echo "  Network partitioned: $ORIGINAL_PRIMARY"
echo "  Partition duration: ${PARTITION_TIME}s"
echo "  Failover detected during partition: $([ "$PASSIVE_ROLE_DURING" = "PRIMARY" ] && echo "Yes" || echo "No/Partial")"
echo "  Final PRIMARY: $FINAL_PRIMARY"
echo "  Final PASSIVE: $FINAL_PASSIVE"
echo "  Split-brain avoided: $([ "$FINAL_NODE1_ROLE" != "PRIMARY" ] || [ "$FINAL_NODE2_ROLE" != "PRIMARY" ] && echo "Yes" || echo "NO!")"
echo ""

# Print test result
if [ "$TEST_PASSED" = true ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
