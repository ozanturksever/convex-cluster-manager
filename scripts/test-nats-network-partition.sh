#!/bin/bash
# test-nats-network-partition.sh
# Test NATS network partition: Isolate one NATS node and verify cluster handles it gracefully
#
# Prerequisites:
#   - Run scripts/test-nats-cluster-setup.sh --keep first to set up the cluster
#
# Test scenario:
#   1. Identify a NATS node to partition
#   2. Disconnect the node from the cluster network
#   3. Verify NATS cluster maintains quorum (2/3 nodes)
#   4. Verify Convex cluster continues to function
#   5. Test data operations during partition
#   6. Reconnect the partitioned node (heal partition)
#   7. Verify full cluster recovery
#   8. Verify data consistency
#
# Usage:
#   ./test-nats-network-partition.sh [OPTIONS]
#
# Options:
#   --partition-time SECS  How long to maintain partition (default: 30)
#   --target-leader        Partition the NATS leader instead of a follower
#   --verbose              Show verbose output
#   --help                 Show help message

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

# Test parameters
PARTITION_TIME=30
TARGET_LEADER=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --partition-time)
            PARTITION_TIME="$2"
            shift 2
            ;;
        --target-leader)
            TARGET_LEADER=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Test NATS network partition scenario"
            echo ""
            echo "Options:"
            echo "  --partition-time SECS  How long to maintain partition (default: 30)"
            echo "  --target-leader        Partition the NATS leader instead of a follower"
            echo "  --verbose              Show verbose output"
            echo "  --help                 Show help message"
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
TEST_NAME="NATS Network Partition Test"
TEST_PASSED=true
PARTITIONED_NODE=""

# Cleanup function to ensure network is restored
cleanup() {
    log_warn "Cleaning up - ensuring network connectivity is restored..."
    
    if [ -n "$PARTITIONED_NODE" ]; then
        heal_nats_partition "$PARTITIONED_NODE" "$NATS_CLUSTER_NETWORK" 2>/dev/null || true
    fi
    
    # Ensure all nodes are connected
    for container in "$NATS_NODE1_CONTAINER" "$NATS_NODE2_CONTAINER" "$NATS_NODE3_CONTAINER"; do
        heal_nats_partition "$container" "$NATS_CLUSTER_NETWORK" 2>/dev/null || true
    done
}

trap cleanup EXIT

print_test_header "$TEST_NAME"

echo -e "${CYAN}Test Scenario:${NC}"
echo "  1. Partition a NATS node from the cluster network"
echo "  2. Maintain partition for ${PARTITION_TIME}s"
echo "  3. Verify NATS cluster maintains quorum"
echo "  4. Verify Convex cluster continues to function"
echo "  5. Heal partition and verify recovery"
echo ""
echo -e "${CYAN}Configuration:${NC}"
echo "  Partition duration: ${PARTITION_TIME}s"
echo "  Target leader: $([ "$TARGET_LEADER" = true ] && echo "YES" || echo "NO (follower)")"
echo ""

# Step 1: Verify infrastructure is running
log_step 1 "Verifying test infrastructure..."

# Check NATS cluster
if ! verify_nats_cluster_health; then
    log_error "NATS cluster not healthy. Run 'test-nats-cluster-setup.sh --keep' first."
    exit 1
fi

# Verify all 3 NATS nodes are healthy
INITIAL_COUNT=$(get_healthy_nats_node_count)
if [ "$INITIAL_COUNT" -ne 3 ]; then
    log_error "Expected 3 healthy NATS nodes, got $INITIAL_COUNT"
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

log_success "Infrastructure verified (3/3 NATS nodes healthy)"

# Step 2: Get current cluster status
log_step 2 "Checking current cluster status..."

print_nats_cluster_status

NATS_LEADER=$(get_nats_leader)
NATS_LEADER_NAME=$(get_nats_leader_name)

if [ -z "$NATS_LEADER" ]; then
    log_error "Could not determine NATS JetStream leader"
    exit 1
fi

log_info "Current NATS JetStream leader: $NATS_LEADER ($NATS_LEADER_NAME)"

# Get Convex cluster status
PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

if [ -z "$PRIMARY_NODE" ]; then
    log_error "No Convex PRIMARY node detected"
    exit 1
fi

log_success "Convex PRIMARY: $PRIMARY_NODE"
log_info "Convex PASSIVE: $PASSIVE_NODE"

# Verify backend
if ! wait_for_backend "$PRIMARY_NODE" 30; then
    log_error "Convex backend not healthy"
    exit 1
fi

# Step 3: Select NATS node to partition
log_step 3 "Selecting NATS node to partition..."

if [ "$TARGET_LEADER" = true ]; then
    PARTITIONED_NODE="$NATS_LEADER"
    log_info "Targeting NATS leader for partition: $PARTITIONED_NODE"
else
    PARTITIONED_NODE=$(get_non_leader_nats_node)
    if [ -z "$PARTITIONED_NODE" ]; then
        log_error "Could not find a non-leader NATS node"
        exit 1
    fi
    log_info "Targeting NATS follower for partition: $PARTITIONED_NODE"
fi

# Step 4: Insert test data before partition
log_step 4 "Inserting test data before network partition..."

TEST_MESSAGE_BEFORE="Before-Partition-$(date +%s)"

if [ -n "$ADMIN_KEY" ]; then
    MUTATION_RESULT=$(call_mutation "$PRIMARY_NODE" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_BEFORE\", \"author\": \"partition-test\"}")
    
    if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
        log_success "Test data inserted: $TEST_MESSAGE_BEFORE"
    else
        log_warn "Could not insert test data (continuing anyway)"
    fi
else
    log_warn "No ADMIN_KEY found, skipping data insertion"
fi

# Step 5: Create network partition
log_step 5 "Creating network partition..."

log_info "Disconnecting $PARTITIONED_NODE from $NATS_CLUSTER_NETWORK"

if partition_nats_node "$PARTITIONED_NODE" "$NATS_CLUSTER_NETWORK"; then
    log_success "Network partition created - $PARTITIONED_NODE is isolated"
else
    log_error "Failed to create network partition"
    TEST_PASSED=false
fi

# Wait for cluster to detect the partition
sleep 5

# Step 6: Verify NATS cluster maintains quorum during partition
log_step 6 "Verifying NATS cluster maintains quorum during partition..."

HEALTHY_COUNT=$(get_healthy_nats_node_count)
log_info "Healthy NATS nodes during partition: $HEALTHY_COUNT/3"

# Note: The partitioned node might still report as healthy via its host port
# but it won't be able to communicate with the cluster

if [ "$HEALTHY_COUNT" -ge 2 ]; then
    log_success "NATS cluster maintains quorum ($HEALTHY_COUNT nodes reachable)"
else
    log_warn "NATS cluster appears to have lost quorum"
fi

# Check if remaining nodes can still see a leader
if [ "$TARGET_LEADER" = true ]; then
    log_info "Waiting for leader re-election (leader was partitioned)..."
    sleep 10
    
    if wait_for_nats_leader_election "$PARTITIONED_NODE" 30; then
        NEW_LEADER=$(get_nats_leader)
        log_success "New leader elected during partition: $NEW_LEADER"
    else
        log_warn "Leader election may still be in progress"
    fi
fi

print_nats_cluster_status

# Step 7: Verify Convex cluster survives partition
log_step 7 "Verifying Convex cluster survives network partition..."

# Wait for any connection issues to settle
sleep 5

# Check if Convex backend is still healthy
if wait_for_backend "$PRIMARY_NODE" 30; then
    log_success "Convex backend still healthy during NATS partition"
else
    log_error "Convex backend not responding during NATS partition"
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

# Step 8: Test data operations during partition
log_step 8 "Testing data operations during network partition..."

if [ -n "$ADMIN_KEY" ] && [ -n "$CURRENT_PRIMARY" ]; then
    TEST_MESSAGE_DURING="During-Partition-$(date +%s)"
    
    MUTATION_RESULT=$(call_mutation "$CURRENT_PRIMARY" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_DURING\", \"author\": \"partition-test\"}")
    
    if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
        log_success "Data insertion during partition: SUCCESS"
    else
        log_warn "Data insertion during partition may have failed: $MUTATION_RESULT"
    fi
else
    log_warn "Skipping data operations test"
fi

# Step 9: Wait for partition duration
log_step 9 "Maintaining network partition for ${PARTITION_TIME}s..."

log_info "Partition started at $(date '+%H:%M:%S')"
log_info "Partition will be healed at $(date -v+${PARTITION_TIME}S '+%H:%M:%S' 2>/dev/null || date -d "+${PARTITION_TIME} seconds" '+%H:%M:%S' 2>/dev/null || echo "~${PARTITION_TIME}s from now")"

# Wait in intervals and check status
ELAPSED=0
INTERVAL=10

while [ $ELAPSED -lt $PARTITION_TIME ]; do
    REMAINING=$((PARTITION_TIME - ELAPSED))
    
    if [ $REMAINING -gt $INTERVAL ]; then
        sleep $INTERVAL
        ELAPSED=$((ELAPSED + INTERVAL))
        
        # Quick status check
        HEALTHY=$(get_healthy_nats_node_count)
        log_info "[$ELAPSED/${PARTITION_TIME}s] NATS nodes reachable: $HEALTHY"
    else
        sleep $REMAINING
        ELAPSED=$PARTITION_TIME
    fi
done

log_success "Partition duration complete"

# Step 10: Heal the network partition
log_step 10 "Healing network partition..."

log_info "Reconnecting $PARTITIONED_NODE to $NATS_CLUSTER_NETWORK"

if heal_nats_partition "$PARTITIONED_NODE" "$NATS_CLUSTER_NETWORK"; then
    log_success "Network partition healed - $PARTITIONED_NODE reconnected"
else
    log_warn "Failed to heal partition cleanly (may already be connected)"
fi

# Step 11: Wait for cluster recovery
log_step 11 "Waiting for full cluster recovery..."

# Wait for cluster to stabilize
sleep 10

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

# Verify NATS has a leader
FINAL_LEADER=$(get_nats_leader)
FINAL_LEADER_NAME=$(get_nats_leader_name)

if [ -n "$FINAL_LEADER" ]; then
    log_success "NATS JetStream leader after recovery: $FINAL_LEADER ($FINAL_LEADER_NAME)"
else
    log_error "No NATS JetStream leader after recovery"
    TEST_PASSED=false
fi

# Verify all 3 nodes are healthy
FINAL_HEALTHY=$(get_healthy_nats_node_count)
if [ "$FINAL_HEALTHY" -eq 3 ]; then
    log_success "All 3 NATS nodes are healthy"
else
    log_error "Not all NATS nodes recovered: $FINAL_HEALTHY/3"
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

# Step 13: Verify data consistency
log_step 13 "Verifying data consistency..."

if [ -n "$ADMIN_KEY" ] && [ -n "$FINAL_PRIMARY" ]; then
    QUERY_RESULT=$(call_query "$FINAL_PRIMARY" "$ADMIN_KEY" "messages:list")
    
    # Check for message from before partition
    if echo "$QUERY_RESULT" | grep -q "$TEST_MESSAGE_BEFORE"; then
        log_success "Data from before partition is accessible"
    else
        log_error "Data from before partition NOT found!"
        TEST_PASSED=false
    fi
    
    # Check for message during partition (if it was created)
    if [ -n "$TEST_MESSAGE_DURING" ]; then
        if echo "$QUERY_RESULT" | grep -q "$TEST_MESSAGE_DURING"; then
            log_success "Data from during partition is accessible"
        else
            log_warn "Data from during partition not found (may have been lost)"
        fi
    fi
    
    # Get final message count
    COUNT_RESULT=$(call_query "$FINAL_PRIMARY" "$ADMIN_KEY" "messages:count")
    log_info "Total message count: $COUNT_RESULT"
fi

# Step 14: Test post-recovery operations
log_step 14 "Testing post-recovery data operations..."

if [ -n "$ADMIN_KEY" ] && [ -n "$FINAL_PRIMARY" ]; then
    TEST_MESSAGE_AFTER="After-Partition-$(date +%s)"
    
    MUTATION_RESULT=$(call_mutation "$FINAL_PRIMARY" "$ADMIN_KEY" "messages:add" "{\"content\": \"$TEST_MESSAGE_AFTER\", \"author\": \"partition-test\"}")
    
    if echo "$MUTATION_RESULT" | grep -q '"status":"success"'; then
        log_success "Post-recovery data insertion: SUCCESS"
    else
        log_error "Post-recovery data insertion failed"
        TEST_PASSED=false
    fi
fi

# Step 15: Print summary
echo ""
echo -e "${CYAN}━━━ Test Summary ━━━${NC}"
echo "  Partitioned NATS node: $PARTITIONED_NODE"
echo "  Partition target: $([ "$TARGET_LEADER" = true ] && echo "LEADER" || echo "FOLLOWER")"
echo "  Partition duration: ${PARTITION_TIME}s"
echo "  NATS quorum maintained: $([ "$HEALTHY_COUNT" -ge 2 ] && echo "YES" || echo "NO")"
echo "  Convex cluster survived: $([ -n "$CURRENT_PRIMARY" ] && echo "YES" || echo "NO")"
echo "  Full recovery achieved: $([ "$FINAL_HEALTHY" -eq 3 ] && echo "YES" || echo "NO")"
echo "  Data consistency: $([ "$TEST_PASSED" = true ] && echo "VERIFIED" || echo "ISSUES DETECTED")"
echo ""

# Print test result
if [ "$TEST_PASSED" = true ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
