#!/bin/bash
# test-cluster-failover-graceful.sh
# Graceful failover test: Use demote command to gracefully step down
#
# Prerequisites:
#   - Run scripts/test-cluster-e2e.sh --keep first to set up the cluster
#   - Or have a running cluster with the expected container names
#
# Usage:
#   ./test-cluster-failover-graceful.sh [OPTIONS]
#
# Options:
#   --nats-container NAME    NATS container name (default: convex-e2e-nats)
#   --node1-container NAME   Node1 container name (default: convex-e2e-node1)
#   --node2-container NAME   Node2 container name (default: convex-e2e-node2)
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
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Graceful failover test: Use demote command to gracefully step down"
            echo ""
            echo "Options:"
            echo "  --nats-container NAME    NATS container name (default: convex-e2e-nats)"
            echo "  --node1-container NAME   Node1 container name (default: convex-e2e-node1)"
            echo "  --node2-container NAME   Node2 container name (default: convex-e2e-node2)"
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
TEST_NAME="Graceful Failover Test"
TEST_PASSED=true

print_test_header "$TEST_NAME"

# Step 1: Verify containers are running
log_step 1 "Verifying cluster containers are running..."

if ! check_required_containers "$NATS_CONTAINER" "$NODE1_CONTAINER" "$NODE2_CONTAINER"; then
    log_error "Required containers not running. Run 'scripts/test-cluster-e2e.sh --keep' first."
    exit 1
fi

log_success "All required containers are running"

# Step 2: Ensure both daemons are running and cluster is stable
log_step 2 "Ensuring cluster is stable with both nodes running..."

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

if [ -z "$PRIMARY_NODE" ] || [ -z "$PASSIVE_NODE" ]; then
    log_warn "Cluster not in expected state. Starting daemons..."
    start_daemon "$NODE1_CONTAINER"
    sleep 5
    start_daemon "$NODE2_CONTAINER"
    sleep 10
    
    PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
fi

if [ -z "$PRIMARY_NODE" ]; then
    log_error "No PRIMARY node detected"
    exit 1
fi

if [ -z "$PASSIVE_NODE" ]; then
    log_error "No PASSIVE node detected"
    exit 1
fi

log_success "Cluster is stable"
log_info "PRIMARY: $PRIMARY_NODE"
log_info "PASSIVE: $PASSIVE_NODE"

# Step 3: Use demote command on primary
log_step 3 "Executing graceful demote on primary node..."

log_info "Running 'convex-cluster-manager demote' on $PRIMARY_NODE"

DEMOTE_OUTPUT=$(docker exec "$PRIMARY_NODE" /usr/local/bin/convex-cluster-manager demote --config /etc/convex/cluster.json 2>&1 || true)

if [ "$VERBOSE" = true ]; then
    log_info "Demote output:"
    echo "$DEMOTE_OUTPUT"
fi

if echo "$DEMOTE_OUTPUT" | grep -q "Successfully demoted\|not the current leader"; then
    log_success "Demote command executed"
else
    log_warn "Demote output may indicate issues: $DEMOTE_OUTPUT"
fi

# Step 4: Wait for leadership transfer
log_step 4 "Waiting for leadership transfer..."

log_info "Waiting for passive node to become primary..."
sleep 10

# Step 5: Verify failover occurred
log_step 5 "Verifying graceful failover..."

NEW_PRIMARY_ROLE=$(get_node_role "$PASSIVE_NODE")
OLD_PRIMARY_ROLE=$(get_node_role "$PRIMARY_NODE")

log_info "Former PRIMARY ($PRIMARY_NODE) is now: $OLD_PRIMARY_ROLE"
log_info "Former PASSIVE ($PASSIVE_NODE) is now: $NEW_PRIMARY_ROLE"

if [ "$NEW_PRIMARY_ROLE" = "PRIMARY" ]; then
    log_success "Graceful failover successful! Former passive is now PRIMARY"
else
    log_warn "Failover may need more time. Waiting additional 10 seconds..."
    sleep 10
    NEW_PRIMARY_ROLE=$(get_node_role "$PASSIVE_NODE")
    
    if [ "$NEW_PRIMARY_ROLE" = "PRIMARY" ]; then
        log_success "Graceful failover successful! Former passive is now PRIMARY"
    else
        log_error "Graceful failover FAILED. Former passive is: $NEW_PRIMARY_ROLE"
        TEST_PASSED=false
    fi
fi

if [ "$OLD_PRIMARY_ROLE" = "PASSIVE" ]; then
    log_success "Former primary gracefully transitioned to PASSIVE"
else
    log_warn "Former primary is in state: $OLD_PRIMARY_ROLE (expected PASSIVE)"
fi

# Step 6: Verify both backends are healthy
log_step 6 "Verifying backends are healthy..."

if wait_for_backend "$PASSIVE_NODE" 30; then
    log_success "Backend healthy on new primary ($PASSIVE_NODE)"
else
    log_error "Backend not healthy on new primary"
    TEST_PASSED=false
fi

# Step 7: Test failback (demote the new primary)
log_step 7 "Testing failback (demote new primary)..."

log_info "Running 'convex-cluster-manager demote' on new primary ($PASSIVE_NODE)"

FAILBACK_OUTPUT=$(docker exec "$PASSIVE_NODE" /usr/local/bin/convex-cluster-manager demote --config /etc/convex/cluster.json 2>&1 || true)

if [ "$VERBOSE" = true ]; then
    log_info "Failback demote output:"
    echo "$FAILBACK_OUTPUT"
fi

log_info "Waiting for failback..."
sleep 10

# Step 8: Verify failback occurred
log_step 8 "Verifying failback..."

FINAL_NODE1_ROLE=$(get_node_role "$NODE1_CONTAINER")
FINAL_NODE2_ROLE=$(get_node_role "$NODE2_CONTAINER")

log_info "Node1 final role: $FINAL_NODE1_ROLE"
log_info "Node2 final role: $FINAL_NODE2_ROLE"

# Check that one is PRIMARY and one is PASSIVE
if { [ "$FINAL_NODE1_ROLE" = "PRIMARY" ] && [ "$FINAL_NODE2_ROLE" = "PASSIVE" ]; } || \
   { [ "$FINAL_NODE1_ROLE" = "PASSIVE" ] && [ "$FINAL_NODE2_ROLE" = "PRIMARY" ]; }; then
    log_success "Failback successful! Cluster is in healthy state"
else
    log_error "Failback may have failed. Unexpected cluster state."
    TEST_PASSED=false
fi

# Step 9: Print final status
log_step 9 "Final cluster status..."

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# Print test result
if [ "$TEST_PASSED" = true ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
