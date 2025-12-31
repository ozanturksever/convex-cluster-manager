#!/bin/bash
# test-nats-single-node-failure.sh
# Test NATS single node failure: Kill one non-leader NATS node and verify cluster survives
#
# Prerequisites:
#   - Run scripts/test-nats-cluster-setup.sh --keep first to set up the cluster
#
# Test scenario:
#   1. Identify a non-leader NATS node
#   2. Kill the non-leader NATS node
#   3. Verify NATS cluster maintains quorum (2/3 nodes)
#   4. Verify Convex cluster continues to function
#   5. Verify WAL replication continues working
#   6. Restart the killed NATS node
#   7. Verify full cluster recovery
#
# Usage:
#   ./test-nats-single-node-failure.sh [OPTIONS]
#
# Options:
#   --verbose        Show verbose output
#   --help           Show help message

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CLUSTER_MGR_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
export PROJECT_ROOT="$(cd "$CLUSTER_MGR_DIR/.." && pwd)"

# Source common libraries
source "$SCRIPT_DIR/failover-tests/lib-failover-common.sh"
source "$SCRIPT_DIR/failover-tests/lib-nats-cluster.sh"

# Configuration
TEST_DIR="$CLUSTER_MGR_DIR/test-nats-cluster-output"
NODE1_CONTAINER="convex-nats-node1"
NODE2_CONTAINER="convex-nats-node2"

# Source test config if available
if [ -f "$TEST_DIR/test-config.env" ]; then
    source "$TEST_DIR/test-config.env"
fi

# Flags
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Test NATS single node failure scenario"
            echo ""
            echo "Options:"
            echo "  --verbose        Show verbose output"
            echo "  --help           Show help message"
            echo ""
            echo "Prerequisites:"
            echo "  Run 'scripts/test-nats-cluster-setup.sh --keep' first"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Test variables
TEST_NAME="NATS Single Node Failure Test"
TEST_PASSED=true

# Cleanup function to ensure NATS node is restarted
cleanup() {
    log_warn "Cleaning up - ensuring all NATS nodes are running..."
    restart_nats_node "$NATS_NODE1_CONTAINER" 2>/dev/null || true
    restart_nats_node "$NATS_NODE2_CONTAINER" 2>/dev/null || true
    restart_nats_node "$NATS_NODE3_CONTAINER" 2>/dev/null || true
}

trap cleanup EXIT

print_test_header "$TEST_NAME"

echo -e "${CYAN}Test Scenario:${NC}"
echo "  1. Kill a non-leader NATS node"
echo "  2. Verify NATS cluster maintains quorum (2/3)"
echo "  3. Verify Convex cluster continues to function"
echo "  4. Restart the killed node"
echo "  5. Verify full cluster recovery"
echo ""

# Step 1: Verify infrastructure is running
log_step 1 "Verifying test infrastructure..."

# Check NATS cluster
if ! verify_nats_cluster_health; then
    log_error "NATS cluster not healthy. Run 'test-nats-cluster-setup.sh --keep' first."
    exit 1
fi

# Check Convex containers
if ! docker ps --format '{{.Names}}' | grep -q "^${NODE1_CONTAINER}$"; then
    log_error "Convex node $NODE1_CONTAINER not running."
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${NODE2_CONTAINER}$"; then
    log_error "Convex node $NODE2_CONTAINER not running."
    exit 1
fi

log_success "Infrastructure verified"

# Step 2: Get current NATS cluster status
log_step 2 "Checking current NATS cluster status..."

print_nats_cluster_status

NATS_LEADER=$(get_nats_leader)
if [ -z "$NATS_LEADER" ]; then
    log_error "Could not determine NATS JetStream leader"
    exit 1
fi

log_info "Current NATS JetStream leader: $NATS_LEADER"

# Step 3: Get current Convex cluster status
log_step 3 "Checking current Convex cluster status..."

PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

if [ -z "$PRIMARY_NODE" ]; then
    log_error "No Convex PRIMARY node detected"
    exit 1
fi

log_success "Convex PRIMARY: $PRIMARY_NODE"
log_info "Convex PASSIVE: $PASSIVE_NODE"

# Verify backend is working
if ! wait_for_backend "$PRIMARY_NODE" 30; then
    log_error "Convex backend not healthy"
    exit 1
fi

log_success "Convex backend is healthy"

# Step 4: Select a non-leader NATS node to kill
log_step 4 "Selecting non-leader NATS node to kill..."

TARGET_NODE=$(get_non_leader_nats_node)
if [ -z "$TARGET_NODE" ]; then
    log_error "Could not find a non-leader NATS node"
    exit 1
fi

log_info "Target NATS node to kill: $TARGET_NODE"
log_info "NATS leader (will remain): $NATS_LEADER"

# Step 5: Insert test data before killing NATS node
log_step 5 "Inserting test data before NATS node failure..."

TEST_MESSAGE_BEFORE="Before-NATS-Kill-$(date +%s)"

if [ -n "$ADMIN_KEY" ]; then
    MUTATION_RESULT=$(call_mutation "$PRIMARY_NODE" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_BEFORE\", \"author\": \"nats-test\"}")
    
    if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
        log_success "Test data inserted: $TEST_MESSAGE_BEFORE"
    else
        log_warn "Could not insert test data (continuing anyway)"
    fi
else
    log_warn "No ADMIN_KEY found, skipping data insertion"
fi

# Step 6: Kill the target NATS node
log_step 6 "Killing NATS node: $TARGET_NODE..."

INITIAL_HEALTHY_COUNT=$(get_healthy_nats_node_count)
log_info "Initial healthy NATS nodes: $INITIAL_HEALTHY_COUNT"

if ! kill_nats_node "$TARGET_NODE"; then
    log_error "Failed to kill NATS node"
    TEST_PASSED=false
fi

# Wait for cluster to detect the failure
sleep 5

# Step 7: Verify NATS cluster maintains quorum
log_step 7 "Verifying NATS cluster maintains quorum..."

HEALTHY_COUNT=$(get_healthy_nats_node_count)
log_info "Healthy NATS nodes after kill: $HEALTHY_COUNT/3"

if [ "$HEALTHY_COUNT" -ge 2 ]; then
    log_success "NATS cluster maintains quorum ($HEALTHY_COUNT/3 nodes)"
else
    log_error "NATS cluster lost quorum! Only $HEALTHY_COUNT/3 nodes healthy"
    TEST_PASSED=false
fi

# Verify NATS cluster is still functional
if verify_nats_cluster_health; then
    log_success "NATS cluster is still functional"
else
    log_error "NATS cluster is not functional"
    TEST_PASSED=false
fi

print_nats_cluster_status

# Step 8: Verify Convex cluster survives
log_step 8 "Verifying Convex cluster survives NATS node failure..."

# Wait for any connection issues to settle
sleep 5

# Check if Convex backend is still healthy
if wait_for_backend "$PRIMARY_NODE" 30; then
    log_success "Convex backend still healthy after NATS node failure"
else
    log_error "Convex backend not responding after NATS node failure"
    TEST_PASSED=false
fi

# Verify Convex cluster status
CURRENT_PRIMARY=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
if [ -n "$CURRENT_PRIMARY" ]; then
    log_success "Convex cluster still has PRIMARY: $CURRENT_PRIMARY"
else
    log_error "Convex cluster lost PRIMARY node"
    TEST_PASSED=false
fi

# Step 9: Test data operations during NATS partial failure
log_step 9 "Testing data operations during NATS partial failure..."

if [ -n "$ADMIN_KEY" ]; then
    TEST_MESSAGE_DURING="During-NATS-Kill-$(date +%s)"
    
    MUTATION_RESULT=$(call_mutation "$PRIMARY_NODE" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_DURING\", \"author\": \"nats-test\"}")
    
    if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
        log_success "Data insertion during NATS partial failure: SUCCESS"
        
        # Query to verify
        QUERY_RESULT=$(call_query "$PRIMARY_NODE" "$ADMIN_KEY" "messages:list")
        if echo "$QUERY_RESULT" | grep -q "$TEST_MESSAGE_DURING"; then
            log_success "Data verified on Convex cluster"
        else
            log_warn "Data not immediately visible (may need replication time)"
        fi
    else
        log_warn "Data insertion during NATS partial failure returned: $MUTATION_RESULT"
    fi
else
    log_warn "No ADMIN_KEY found, skipping data operations test"
fi

# Step 10: Restart the killed NATS node
log_step 10 "Restarting killed NATS node: $TARGET_NODE..."

if restart_nats_node "$TARGET_NODE"; then
    log_success "NATS node $TARGET_NODE restarted"
else
    log_error "Failed to restart NATS node $TARGET_NODE"
    TEST_PASSED=false
fi

# Step 11: Wait for full cluster recovery
log_step 11 "Waiting for full NATS cluster recovery..."

if wait_for_full_nats_cluster 60; then
    log_success "Full NATS cluster recovered (3/3 nodes)"
else
    log_error "NATS cluster did not fully recover"
    TEST_PASSED=false
fi

# Wait for JetStream to stabilize
sleep 5

# Step 12: Verify final cluster state
log_step 12 "Verifying final cluster state..."

print_nats_cluster_status

# Verify NATS leader
FINAL_LEADER=$(get_nats_leader)
if [ -n "$FINAL_LEADER" ]; then
    log_success "NATS JetStream leader: $FINAL_LEADER"
else
    log_error "No NATS JetStream leader after recovery"
    TEST_PASSED=false
fi

# Verify Convex cluster
FINAL_PRIMARY=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
FINAL_PASSIVE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

if [ -n "$FINAL_PRIMARY" ] && [ -n "$FINAL_PASSIVE" ]; then
    log_success "Convex cluster healthy: PRIMARY=$FINAL_PRIMARY, PASSIVE=$FINAL_PASSIVE"
else
    log_error "Convex cluster not in expected state"
    TEST_PASSED=false
fi

# Verify all data is accessible
if [ -n "$ADMIN_KEY" ]; then
    QUERY_RESULT=$(call_query "$FINAL_PRIMARY" "$ADMIN_KEY" "messages:list")
    
    if echo "$QUERY_RESULT" | grep -q "$TEST_MESSAGE_BEFORE"; then
        log_success "Data from before NATS kill is accessible"
    else
        log_warn "Data from before NATS kill not found"
    fi
fi

# Step 13: Print summary
echo ""
echo -e "${CYAN}━━━ Test Summary ━━━${NC}"
echo "  Target NATS node killed: $TARGET_NODE"
echo "  NATS quorum maintained: $([ "$HEALTHY_COUNT" -ge 2 ] && echo "YES" || echo "NO")"
echo "  Convex cluster survived: $([ -n "$CURRENT_PRIMARY" ] && echo "YES" || echo "NO")"
echo "  Full recovery achieved: $([ "$(get_healthy_nats_node_count)" -eq 3 ] && echo "YES" || echo "NO")"
echo ""

# Print test result
if [ "$TEST_PASSED" = true ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
