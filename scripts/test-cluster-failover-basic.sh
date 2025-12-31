#!/bin/bash
# test-cluster-failover-basic.sh
# Basic failover test: Kill primary daemon and verify passive takes over
#
# Prerequisites:
#   - Run scripts/test-cluster-e2e.sh --keep first to set up the cluster
#   - Or have a running cluster with the expected container names
#
# Usage:
#   ./test-cluster-failover-basic.sh [OPTIONS]
#
# Options:
#   --nats-container NAME    NATS container name (default: convex-e2e-nats)
#   --node1-container NAME   Node1 container name (default: convex-e2e-node1)
#   --node2-container NAME   Node2 container name (default: convex-e2e-node2)
#   --admin-key KEY          Admin key for Convex API calls
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
ADMIN_KEY=""
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
        --admin-key)
            ADMIN_KEY="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Basic failover test: Kill primary daemon and verify passive takes over"
            echo ""
            echo "Options:"
            echo "  --nats-container NAME    NATS container name (default: convex-e2e-nats)"
            echo "  --node1-container NAME   Node1 container name (default: convex-e2e-node1)"
            echo "  --node2-container NAME   Node2 container name (default: convex-e2e-node2)"
            echo "  --admin-key KEY          Admin key for Convex API calls"
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
TEST_NAME="Basic Failover Test"
TEST_PASSED=true

print_test_header "$TEST_NAME"

# Step 1: Verify containers are running
log_step 1 "Verifying cluster containers are running..."

if ! check_required_containers "$NATS_CONTAINER" "$NODE1_CONTAINER" "$NODE2_CONTAINER"; then
    log_error "Required containers not running. Run 'scripts/test-cluster-e2e.sh --keep' first."
    exit 1
fi

log_success "All required containers are running"

# Step 2: Get current cluster status
log_step 2 "Checking current cluster status..."

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

if [ -z "$PRIMARY_NODE" ]; then
    log_error "No PRIMARY node detected. Ensure daemons are running."
    log_info "Starting daemons on both nodes..."
    start_daemon "$NODE1_CONTAINER"
    sleep 5
    start_daemon "$NODE2_CONTAINER"
    sleep 10
    
    PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    
    if [ -z "$PRIMARY_NODE" ]; then
        log_error "Still no PRIMARY node detected after starting daemons."
        exit 1
    fi
fi

log_success "PRIMARY node: $PRIMARY_NODE"
log_info "PASSIVE node: $PASSIVE_NODE"

# Step 3: Verify backend is working on primary
log_step 3 "Verifying backend is healthy on primary..."

if wait_for_backend "$PRIMARY_NODE" 30; then
    log_success "Backend is healthy on primary"
else
    log_error "Backend not healthy on primary"
    TEST_PASSED=false
fi

# Step 4: Kill the primary daemon
log_step 4 "Killing cluster daemon on primary node..."

log_info "Killing daemon on $PRIMARY_NODE"
kill_daemon "$PRIMARY_NODE"
log_success "Daemon killed on primary"

# Step 5: Wait for failover
log_step 5 "Waiting for failover (TTL expiration ~15 seconds)..."

log_info "Waiting for passive node to detect leader loss and take over..."
sleep 15

# Step 6: Verify failover occurred
log_step 6 "Verifying failover..."

NEW_PRIMARY_ROLE=$(get_node_role "$PASSIVE_NODE")

if [ "$NEW_PRIMARY_ROLE" = "PRIMARY" ]; then
    log_success "Failover successful! Former passive ($PASSIVE_NODE) is now PRIMARY"
else
    log_warn "Failover may still be in progress. Waiting additional 10 seconds..."
    sleep 10
    NEW_PRIMARY_ROLE=$(get_node_role "$PASSIVE_NODE")
    
    if [ "$NEW_PRIMARY_ROLE" = "PRIMARY" ]; then
        log_success "Failover successful! Former passive ($PASSIVE_NODE) is now PRIMARY"
    else
        log_error "Failover FAILED. Former passive is still: $NEW_PRIMARY_ROLE"
        TEST_PASSED=false
    fi
fi

# Step 7: Verify backend is working on new primary
log_step 7 "Verifying backend on new primary..."

if [ "$NEW_PRIMARY_ROLE" = "PRIMARY" ]; then
    if wait_for_backend "$PASSIVE_NODE" 60; then
        log_success "Backend is healthy on new primary"
    else
        log_error "Backend not healthy on new primary"
        TEST_PASSED=false
    fi
fi

# Step 8: Print final status
log_step 8 "Final cluster status..."

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# Step 9: Restore the original primary (optional)
log_step 9 "Restarting daemon on former primary..."

start_daemon "$PRIMARY_NODE"
log_info "Daemon restarted on former primary. It should become PASSIVE."
sleep 5

if wait_for_passive "$PRIMARY_NODE" 20; then
    log_success "Former primary is now PASSIVE"
else
    log_warn "Former primary may need more time to sync"
fi

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# Print test result
if [ "$TEST_PASSED" = true ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
