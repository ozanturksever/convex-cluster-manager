#!/bin/bash
# test-nats-leader-failure.sh
# Test NATS JetStream leader failure: Kill the NATS leader and verify cluster elects new leader
#
# Prerequisites:
#   - Run scripts/test-nats-cluster-setup.sh --keep first to set up the cluster
#
# Test scenario:
#   1. Identify the current NATS JetStream meta leader
#   2. Kill the leader NATS node
#   3. Verify NATS cluster elects a new leader
#   4. Verify NATS cluster maintains quorum
#   5. Verify Convex cluster survives the leader transition
#   6. Verify data integrity
#   7. Restart the killed node
#   8. Verify full cluster recovery
#
# Usage:
#   ./test-nats-leader-failure.sh [OPTIONS]
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
            echo "Test NATS JetStream leader failure scenario"
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
TEST_NAME="NATS JetStream Leader Failure Test"
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
echo "  1. Identify NATS JetStream meta leader"
echo "  2. Kill the leader NATS node"
echo "  3. Verify new leader election"
echo "  4. Verify Convex cluster survives"
echo "  5. Test data operations"
echo "  6. Restart killed node"
echo "  7. Verify full recovery"
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

# Step 2: Get current NATS JetStream leader
log_step 2 "Identifying current NATS JetStream meta leader..."

print_nats_cluster_status

ORIGINAL_LEADER=$(get_nats_leader)
ORIGINAL_LEADER_NAME=$(get_nats_leader_name)

if [ -z "$ORIGINAL_LEADER" ]; then
    log_error "Could not determine NATS JetStream leader"
    exit 1
fi

log_success "Current NATS JetStream leader: $ORIGINAL_LEADER ($ORIGINAL_LEADER_NAME)"

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

# Step 4: Insert test data before killing NATS leader
log_step 4 "Inserting test data before NATS leader failure..."

TEST_MESSAGE_BEFORE="Before-Leader-Kill-$(date +%s)"

if [ -n "$ADMIN_KEY" ]; then
    MUTATION_RESULT=$(call_mutation "$PRIMARY_NODE" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_BEFORE\", \"author\": \"nats-leader-test\"}")
    
    if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
        log_success "Test data inserted: $TEST_MESSAGE_BEFORE"
    else
        log_warn "Could not insert test data (continuing anyway)"
    fi
else
    log_warn "No ADMIN_KEY found, skipping data insertion"
fi

# Step 5: Kill the NATS leader
log_step 5 "Killing NATS JetStream leader: $ORIGINAL_LEADER..."

INITIAL_HEALTHY_COUNT=$(get_healthy_nats_node_count)
log_info "Initial healthy NATS nodes: $INITIAL_HEALTHY_COUNT"

if ! kill_nats_node "$ORIGINAL_LEADER"; then
    log_error "Failed to kill NATS leader"
    TEST_PASSED=false
fi

log_success "NATS leader $ORIGINAL_LEADER killed"

# Step 6: Wait for new leader election
log_step 6 "Waiting for new NATS JetStream leader election..."

# Give the cluster time to detect failure and elect new leader
sleep 5

if wait_for_nats_leader_election "$ORIGINAL_LEADER" 30; then
    NEW_LEADER=$(get_nats_leader)
    NEW_LEADER_NAME=$(get_nats_leader_name)
    log_success "New NATS JetStream leader elected: $NEW_LEADER ($NEW_LEADER_NAME)"
    
    if [ "$NEW_LEADER" = "$ORIGINAL_LEADER" ]; then
        log_error "New leader is the same as killed leader - this shouldn't happen!"
        TEST_PASSED=false
    else
        log_success "Leader successfully changed from $ORIGINAL_LEADER_NAME to $NEW_LEADER_NAME"
    fi
else
    log_error "New leader election failed or timed out"
    TEST_PASSED=false
fi

# Step 7: Verify NATS cluster maintains quorum
log_step 7 "Verifying NATS cluster maintains quorum..."

HEALTHY_COUNT=$(get_healthy_nats_node_count)
log_info "Healthy NATS nodes after leader kill: $HEALTHY_COUNT/3"

if [ "$HEALTHY_COUNT" -ge 2 ]; then
    log_success "NATS cluster maintains quorum ($HEALTHY_COUNT/3 nodes)"
else
    log_error "NATS cluster lost quorum! Only $HEALTHY_COUNT/3 nodes healthy"
    TEST_PASSED=false
fi

print_nats_cluster_status

# Step 8: Verify Convex cluster survives leader transition
log_step 8 "Verifying Convex cluster survives NATS leader transition..."

# Wait for connections to stabilize after leader change
log_info "Waiting for connections to stabilize..."
sleep 10

# Check if Convex backend is still healthy
if wait_for_backend "$PRIMARY_NODE" 60; then
    log_success "Convex backend still healthy after NATS leader change"
else
    log_error "Convex backend not responding after NATS leader change"
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

# Step 9: Test data operations after leader change
log_step 9 "Testing data operations after NATS leader change..."

if [ -n "$ADMIN_KEY" ] && [ -n "$CURRENT_PRIMARY" ]; then
    TEST_MESSAGE_AFTER="After-Leader-Kill-$(date +%s)"
    
    # Try mutation with retries (NATS may need time to stabilize)
    MAX_RETRIES=3
    MUTATION_SUCCESS=false
    
    for i in $(seq 1 $MAX_RETRIES); do
        MUTATION_RESULT=$(call_mutation "$CURRENT_PRIMARY" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_AFTER\", \"author\": \"nats-leader-test\"}")
        
        if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
            MUTATION_SUCCESS=true
            break
        fi
        
        log_info "Retry $i/$MAX_RETRIES: Mutation not yet successful, waiting..."
        sleep 3
    done
    
    if [ "$MUTATION_SUCCESS" = true ]; then
        log_success "Data insertion after NATS leader change: SUCCESS"
        
        # Verify both messages exist
        QUERY_RESULT=$(call_query "$CURRENT_PRIMARY" "$ADMIN_KEY" "messages:list")
        
        if echo "$QUERY_RESULT" | grep -q "$TEST_MESSAGE_BEFORE"; then
            log_success "Data from before leader kill is still accessible"
        else
            log_error "Data from before leader kill is NOT accessible!"
            TEST_PASSED=false
        fi
        
        if echo "$QUERY_RESULT" | grep -q "$TEST_MESSAGE_AFTER"; then
            log_success "Data from after leader kill is accessible"
        else
            log_warn "Data from after leader kill not immediately visible"
        fi
    else
        log_error "Data insertion after NATS leader change failed after $MAX_RETRIES retries"
        TEST_PASSED=false
    fi
else
    log_warn "Skipping data operations test (no ADMIN_KEY or PRIMARY)"
fi

# Step 10: Restart the killed NATS leader
log_step 10 "Restarting killed NATS node: $ORIGINAL_LEADER..."

if restart_nats_node "$ORIGINAL_LEADER"; then
    log_success "NATS node $ORIGINAL_LEADER restarted"
else
    log_error "Failed to restart NATS node $ORIGINAL_LEADER"
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

# Verify NATS has a leader (may or may not be the original)
FINAL_LEADER=$(get_nats_leader)
FINAL_LEADER_NAME=$(get_nats_leader_name)

if [ -n "$FINAL_LEADER" ]; then
    log_success "NATS JetStream leader after recovery: $FINAL_LEADER ($FINAL_LEADER_NAME)"
    
    if [ "$FINAL_LEADER_NAME" = "$ORIGINAL_LEADER_NAME" ]; then
        log_info "Original leader $ORIGINAL_LEADER_NAME reclaimed leadership"
    else
        log_info "Leadership remains with $FINAL_LEADER_NAME"
    fi
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

# Final data verification
if [ -n "$ADMIN_KEY" ] && [ -n "$FINAL_PRIMARY" ]; then
    log_info "Final data verification..."
    QUERY_RESULT=$(call_query "$FINAL_PRIMARY" "$ADMIN_KEY" "messages:count")
    log_info "Message count: $QUERY_RESULT"
fi

# Step 13: Print summary
echo ""
echo -e "${CYAN}━━━ Test Summary ━━━${NC}"
echo "  Original NATS leader: $ORIGINAL_LEADER ($ORIGINAL_LEADER_NAME)"
echo "  Leader killed: YES"
echo "  New leader elected: $([ -n "$NEW_LEADER" ] && echo "$NEW_LEADER_NAME" || echo "FAILED")"
echo "  NATS quorum maintained: $([ "$HEALTHY_COUNT" -ge 2 ] && echo "YES" || echo "NO")"
echo "  Convex cluster survived: $([ -n "$CURRENT_PRIMARY" ] && echo "YES" || echo "NO")"
echo "  Data integrity preserved: $([ "$MUTATION_SUCCESS" = true ] && echo "YES" || echo "UNKNOWN")"
echo "  Full recovery achieved: $([ "$(get_healthy_nats_node_count)" -eq 3 ] && echo "YES" || echo "NO")"
echo "  Final NATS leader: $([ -n "$FINAL_LEADER_NAME" ] && echo "$FINAL_LEADER_NAME" || echo "NONE")"
echo ""

# Print test result
if [ "$TEST_PASSED" = true ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
