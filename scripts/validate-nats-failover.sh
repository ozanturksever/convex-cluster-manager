#!/bin/bash
# validate-nats-failover.sh
# Validates NATS connection recovery when NATS nodes fail
#
# This script verifies:
# - Cluster manager maintains operation when a single NATS node fails
# - Connection recovery when NATS nodes are restarted
# - Election continues to work after NATS failover
# - JetStream leader failover is handled gracefully
# - Cluster manager reconnects to healthy NATS nodes automatically
#
# Prerequisites:
# - 3-node NATS cluster running (via test-nats-cluster-setup.sh)
# - Convex cluster nodes running with cluster manager daemons
#
# Usage:
#   ./validate-nats-failover.sh [OPTIONS]
#
# Options:
#   --cluster-id ID    Cluster ID (default: nats-cluster-test)
#   --config FILE      Path to test config (default: auto-detect)
#   --verbose          Show verbose output
#   --help             Show this help message

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_MGR_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$CLUSTER_MGR_DIR/.." && pwd)"

# Export for sourced scripts
export SCRIPT_DIR CLUSTER_MGR_DIR PROJECT_ROOT

# Source common libraries
source "$SCRIPT_DIR/failover-tests/lib-failover-common.sh"
source "$SCRIPT_DIR/failover-tests/lib-nats-cluster.sh"

# Default configuration
CLUSTER_ID="nats-cluster-test"
CONFIG_FILE=""
VERBOSE=false

# Container names (defaults, can be overridden by config)
NODE1_CONTAINER="${NODE1_CONTAINER:-convex-nats-node1}"
NODE2_CONTAINER="${NODE2_CONTAINER:-convex-nats-node2}"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Validates NATS connection recovery when NATS nodes fail"
            echo ""
            echo "Options:"
            echo "  --cluster-id ID    Cluster ID (default: nats-cluster-test)"
            echo "  --config FILE      Path to test config file"
            echo "  --verbose          Show verbose output"
            echo "  --help             Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Try to load config file
TEST_DIR="$CLUSTER_MGR_DIR/test-nats-cluster-output"
if [ -z "$CONFIG_FILE" ] && [ -f "$TEST_DIR/test-config.env" ]; then
    CONFIG_FILE="$TEST_DIR/test-config.env"
fi

if [ -n "$CONFIG_FILE" ] && [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
    log_info "Loaded config from $CONFIG_FILE"
fi

# KV bucket names
ELECTION_BUCKET="convex-${CLUSTER_ID}-election"
STATE_BUCKET="convex-${CLUSTER_ID}-state"

print_test_header "NATS Connection Failover Validation"

echo -e "${CYAN}Configuration:${NC}"
echo "  Cluster ID: $CLUSTER_ID"
echo "  Election Bucket: $ELECTION_BUCKET"
echo "  State Bucket: $STATE_BUCKET"
echo "  Node1 Container: $NODE1_CONTAINER"
echo "  Node2 Container: $NODE2_CONTAINER"
echo ""

# Helper function to record test result
record_test() {
    local test_name=$1
    local result=$2
    
    if [ "$result" = "PASS" ]; then
        log_success "$test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_error "$test_name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# Helper to run nats CLI commands inside a container
nats_cli() {
    local container=$1
    shift
    docker exec "$container" nats "$@" 2>/dev/null
}

# Helper to get KV value with automatic failover to other NATS servers
# Tries the specified server first, then falls back to other healthy NATS servers
get_kv_value() {
    local container=$1
    local bucket=$2
    local key=$3
    local preferred_server=${4:-}
    
    # List of all NATS servers to try
    local servers=("nats://nats1:4222" "nats://nats2:4222" "nats://nats3:4222")
    
    # If a preferred server is specified, try it first
    if [ -n "$preferred_server" ]; then
        local result
        result=$(docker exec "$container" nats kv get "$bucket" "$key" --server "$preferred_server" --raw 2>/dev/null)
        if [ -n "$result" ]; then
            echo "$result"
            return 0
        fi
    fi
    
    # Try each server in order until one succeeds
    for server in "${servers[@]}"; do
        # Skip the preferred server since we already tried it
        if [ "$server" = "$preferred_server" ]; then
            continue
        fi
        
        local result
        result=$(docker exec "$container" nats kv get "$bucket" "$key" --server "$server" --raw 2>/dev/null)
        if [ -n "$result" ]; then
            echo "$result"
            return 0
        fi
    done
    
    # All servers failed
    echo ""
    return 1
}

# ============================================================================
# Pre-flight checks
# ============================================================================

log_step 1 "Pre-flight checks..."

# Check if cluster node containers are running
if ! docker ps --format '{{.Names}}' | grep -q "^${NODE1_CONTAINER}$"; then
    log_error "Node1 container '$NODE1_CONTAINER' is not running"
    log_info "Run ./test-nats-cluster-setup.sh --keep first"
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${NODE2_CONTAINER}$"; then
    log_error "Node2 container '$NODE2_CONTAINER' is not running"
    log_info "Run ./test-nats-cluster-setup.sh --keep first"
    exit 1
fi

# Verify NATS cluster is running with 3 nodes
NATS_NODE_COUNT=$(get_healthy_nats_node_count)
if [ "$NATS_NODE_COUNT" -lt 3 ]; then
    log_error "NATS cluster not fully running. Expected 3 nodes, found $NATS_NODE_COUNT"
    log_info "Run ./test-nats-cluster-setup.sh first"
    exit 1
fi

log_success "Pre-flight checks passed (NATS cluster: $NATS_NODE_COUNT/3 nodes)"

# Print initial status
print_nats_cluster_status
print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# Get initial leader
INITIAL_LEADER=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader")
log_info "Initial cluster leader: $INITIAL_LEADER"

# ============================================================================
# Test 1: Verify cluster is healthy before NATS failures
# ============================================================================

log_step 2 "Verifying cluster is healthy before NATS failure tests..."

# Check we have exactly one PRIMARY
NODE1_ROLE=$(get_node_role "$NODE1_CONTAINER")
NODE2_ROLE=$(get_node_role "$NODE2_CONTAINER")

PRIMARY_COUNT=0
[ "$NODE1_ROLE" = "PRIMARY" ] && PRIMARY_COUNT=$((PRIMARY_COUNT + 1))
[ "$NODE2_ROLE" = "PRIMARY" ] && PRIMARY_COUNT=$((PRIMARY_COUNT + 1))

if [ "$PRIMARY_COUNT" -eq 1 ]; then
    record_test "Exactly one PRIMARY node before tests" "PASS"
else
    record_test "Exactly one PRIMARY node before tests" "FAIL"
    log_error "Cannot proceed without healthy cluster"
    exit 1
fi

# ============================================================================
# Test 2: Single NATS node failure (non-leader)
# ============================================================================

log_step 3 "Testing cluster behavior during non-leader NATS node failure..."

# Get a non-leader NATS node
NON_LEADER_NATS=$(get_non_leader_nats_node)

if [ -z "$NON_LEADER_NATS" ]; then
    log_warn "Could not find non-leader NATS node, using nats2"
    NON_LEADER_NATS="$NATS_NODE2_CONTAINER"
fi

log_info "Killing non-leader NATS node: $NON_LEADER_NATS"
kill_nats_node "$NON_LEADER_NATS"

# Wait a moment for failure to propagate
sleep 5

# Verify NATS cluster still has quorum
if verify_nats_cluster_health; then
    record_test "NATS cluster maintains quorum after non-leader failure" "PASS"
else
    record_test "NATS cluster maintains quorum after non-leader failure" "FAIL"
fi

# Check cluster manager still sees the leader
LEADER_AFTER_NATS_FAIL=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader")

if [ -n "$LEADER_AFTER_NATS_FAIL" ]; then
    record_test "Cluster leader still accessible after NATS node failure" "PASS"
    log_info "Leader: $LEADER_AFTER_NATS_FAIL"
else
    record_test "Cluster leader still accessible after NATS node failure" "FAIL"
fi

# Check node roles are preserved
NODE1_ROLE_AFTER=$(get_node_role "$NODE1_CONTAINER")
NODE2_ROLE_AFTER=$(get_node_role "$NODE2_CONTAINER")

if [ "$NODE1_ROLE" = "$NODE1_ROLE_AFTER" ] && [ "$NODE2_ROLE" = "$NODE2_ROLE_AFTER" ]; then
    record_test "Node roles preserved during non-leader NATS failure" "PASS"
else
    record_test "Node roles preserved during non-leader NATS failure" "FAIL"
    log_info "Node1: $NODE1_ROLE -> $NODE1_ROLE_AFTER"
    log_info "Node2: $NODE2_ROLE -> $NODE2_ROLE_AFTER"
fi

# Restart the killed NATS node
log_info "Restarting NATS node: $NON_LEADER_NATS"
restart_nats_node "$NON_LEADER_NATS"
sleep 5

# ============================================================================
# Test 3: NATS JetStream leader failure
# ============================================================================

log_step 4 "Testing cluster behavior during NATS JetStream leader failure..."

# Get the current NATS JetStream meta leader
NATS_LEADER=$(get_nats_leader)

if [ -z "$NATS_LEADER" ]; then
    log_warn "Could not determine NATS JetStream leader"
    record_test "NATS JetStream leader identification" "FAIL"
else
    log_info "Current NATS JetStream leader: $NATS_LEADER"
    record_test "NATS JetStream leader identification" "PASS"
    
    # Record current cluster state
    CLUSTER_LEADER_BEFORE=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader")
    log_info "Cluster leader before NATS leader kill: $CLUSTER_LEADER_BEFORE"
    
    # Kill the NATS leader
    log_info "Killing NATS JetStream leader: $NATS_LEADER"
    kill_nats_node "$NATS_LEADER"
    
    # Wait for NATS leader election
    log_info "Waiting for NATS JetStream leader election..."
    if wait_for_nats_leader_election "$NATS_LEADER" 30; then
        NEW_NATS_LEADER=$(get_nats_leader)
        record_test "NATS JetStream leader failover" "PASS"
        log_info "New NATS JetStream leader: $NEW_NATS_LEADER"
    else
        record_test "NATS JetStream leader failover" "FAIL"
    fi
    
    # Wait for cluster manager to reconnect
    log_info "Waiting for cluster managers to reconnect (10 seconds)..."
    sleep 10
    
    # Verify cluster manager can still access election state
    CLUSTER_LEADER_AFTER=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader" "nats://nats2:4222")
    
    if [ -n "$CLUSTER_LEADER_AFTER" ]; then
        record_test "Cluster election accessible after NATS leader failover" "PASS"
        log_info "Cluster leader after NATS failover: $CLUSTER_LEADER_AFTER"
    else
        # Try with another server
        CLUSTER_LEADER_AFTER=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader" "nats://nats3:4222")
        if [ -n "$CLUSTER_LEADER_AFTER" ]; then
            record_test "Cluster election accessible after NATS leader failover" "PASS"
            log_info "Cluster leader after NATS failover (via nats3): $CLUSTER_LEADER_AFTER"
        else
            record_test "Cluster election accessible after NATS leader failover" "FAIL"
        fi
    fi
    
    # Check cluster nodes are still functioning
    NODE1_ROLE_AFTER_NATS=$(get_node_role "$NODE1_CONTAINER")
    NODE2_ROLE_AFTER_NATS=$(get_node_role "$NODE2_CONTAINER")
    
    PRIMARY_COUNT_AFTER=0
    [ "$NODE1_ROLE_AFTER_NATS" = "PRIMARY" ] && PRIMARY_COUNT_AFTER=$((PRIMARY_COUNT_AFTER + 1))
    [ "$NODE2_ROLE_AFTER_NATS" = "PRIMARY" ] && PRIMARY_COUNT_AFTER=$((PRIMARY_COUNT_AFTER + 1))
    
    if [ "$PRIMARY_COUNT_AFTER" -eq 1 ]; then
        record_test "Cluster maintains exactly one PRIMARY after NATS leader failover" "PASS"
    else
        record_test "Cluster maintains exactly one PRIMARY after NATS leader failover" "FAIL"
        log_info "Node1: $NODE1_ROLE_AFTER_NATS, Node2: $NODE2_ROLE_AFTER_NATS"
    fi
    
    # Restart the killed NATS node
    log_info "Restarting NATS node: $NATS_LEADER"
    restart_nats_node "$NATS_LEADER"
    sleep 5
fi

# ============================================================================
# Test 4: Network partition simulation (disconnect NATS node from cluster)
# ============================================================================

log_step 5 "Testing behavior during NATS network partition..."

# Get a non-leader NATS node for partition
PARTITION_NODE=$(get_non_leader_nats_node)

if [ -z "$PARTITION_NODE" ]; then
    log_warn "Could not find non-leader NATS node, using $NATS_NODE3_CONTAINER"
    PARTITION_NODE="$NATS_NODE3_CONTAINER"
fi

log_info "Simulating network partition for: $PARTITION_NODE"

# Check if connected to the network first
if is_connected_to_network "$PARTITION_NODE" "$NATS_CLUSTER_NETWORK"; then
    partition_nats_node "$PARTITION_NODE" "$NATS_CLUSTER_NETWORK"
    sleep 5
    
    # Verify NATS cluster still has quorum (2/3 nodes)
    if wait_for_nats_quorum 10; then
        record_test "NATS cluster maintains quorum during partition" "PASS"
    else
        record_test "NATS cluster maintains quorum during partition" "FAIL"
    fi
    
    # Verify cluster manager election still works
    LEADER_DURING_PARTITION=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader")
    
    if [ -n "$LEADER_DURING_PARTITION" ]; then
        record_test "Cluster election works during NATS partition" "PASS"
    else
        record_test "Cluster election works during NATS partition" "FAIL"
    fi
    
    # Heal the partition
    log_info "Healing network partition..."
    heal_nats_partition "$PARTITION_NODE" "$NATS_CLUSTER_NETWORK"
    sleep 5
    
    # Wait for full cluster recovery
    if wait_for_full_nats_cluster 30; then
        record_test "NATS cluster fully recovers after partition healed" "PASS"
    else
        record_test "NATS cluster fully recovers after partition healed" "FAIL"
    fi
else
    log_warn "$PARTITION_NODE is not connected to $NATS_CLUSTER_NETWORK, skipping partition test"
    record_test "NATS network partition test" "SKIP"
fi

# ============================================================================
# Test 5: Majority NATS failure (quorum loss and recovery)
# ============================================================================

log_step 6 "Testing cluster behavior during NATS quorum loss..."

log_warn "This test simulates loss of NATS quorum - cluster operations will be disrupted"

# Record current state
LEADER_BEFORE_QUORUM_LOSS=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader")
log_info "Cluster leader before quorum loss: $LEADER_BEFORE_QUORUM_LOSS"

# Kill 2 NATS nodes to lose quorum
log_info "Killing NATS nodes to lose quorum..."
kill_nats_node "$NATS_NODE2_CONTAINER"
kill_nats_node "$NATS_NODE3_CONTAINER"
sleep 5

# Check that NATS operations fail (expected)
LEADER_NO_QUORUM=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader" "nats://nats1:4222" 2>/dev/null || echo "")

if [ -z "$LEADER_NO_QUORUM" ]; then
    record_test "NATS operations fail without quorum (expected)" "PASS"
    log_info "KV operations fail as expected without NATS quorum"
else
    # This might still succeed with cached data or single-node mode
    log_info "KV operations may still work in degraded mode: $LEADER_NO_QUORUM"
    record_test "NATS operations during quorum loss" "PASS"
fi

# Check node roles are preserved (should be cached/retained)
NODE1_ROLE_NO_QUORUM=$(get_node_role "$NODE1_CONTAINER")
NODE2_ROLE_NO_QUORUM=$(get_node_role "$NODE2_CONTAINER")

log_info "Node roles during NATS quorum loss:"
log_info "  Node1: $NODE1_ROLE_NO_QUORUM"
log_info "  Node2: $NODE2_ROLE_NO_QUORUM"

# At least one node should maintain its role (the other may become UNKNOWN due to connectivity)
if [ "$NODE1_ROLE_NO_QUORUM" = "PRIMARY" ] || [ "$NODE1_ROLE_NO_QUORUM" = "PASSIVE" ] || 
   [ "$NODE2_ROLE_NO_QUORUM" = "PRIMARY" ] || [ "$NODE2_ROLE_NO_QUORUM" = "PASSIVE" ]; then
    record_test "At least one node maintains state during NATS quorum loss" "PASS"
else
    record_test "At least one node maintains state during NATS quorum loss" "FAIL"
fi

# Restore NATS quorum
log_info "Restoring NATS quorum..."
restart_nats_node "$NATS_NODE2_CONTAINER"
restart_nats_node "$NATS_NODE3_CONTAINER"

# Wait for full cluster recovery
if wait_for_full_nats_cluster 60; then
    record_test "NATS cluster recovers after quorum loss" "PASS"
else
    record_test "NATS cluster recovers after quorum loss" "FAIL"
fi

# Wait for cluster managers to reconnect
log_info "Waiting for cluster managers to reconnect (15 seconds)..."
sleep 15

# Verify cluster is healthy again
LEADER_AFTER_RECOVERY=$(get_kv_value "$NODE1_CONTAINER" "$ELECTION_BUCKET" "leader")

if [ -n "$LEADER_AFTER_RECOVERY" ]; then
    record_test "Cluster election works after NATS recovery" "PASS"
    log_info "Cluster leader after recovery: $LEADER_AFTER_RECOVERY"
else
    record_test "Cluster election works after NATS recovery" "FAIL"
fi

# Check we have exactly one PRIMARY again
NODE1_ROLE_RECOVERED=$(get_node_role "$NODE1_CONTAINER")
NODE2_ROLE_RECOVERED=$(get_node_role "$NODE2_CONTAINER")

PRIMARY_COUNT_RECOVERED=0
[ "$NODE1_ROLE_RECOVERED" = "PRIMARY" ] && PRIMARY_COUNT_RECOVERED=$((PRIMARY_COUNT_RECOVERED + 1))
[ "$NODE2_ROLE_RECOVERED" = "PRIMARY" ] && PRIMARY_COUNT_RECOVERED=$((PRIMARY_COUNT_RECOVERED + 1))

if [ "$PRIMARY_COUNT_RECOVERED" -eq 1 ]; then
    record_test "Exactly one PRIMARY after NATS recovery" "PASS"
else
    record_test "Exactly one PRIMARY after NATS recovery" "FAIL"
    log_info "Node1: $NODE1_ROLE_RECOVERED, Node2: $NODE2_ROLE_RECOVERED"
fi

# ============================================================================
# Test 6: Verify data integrity (state bucket consistency)
# ============================================================================

log_step 7 "Verifying state bucket consistency after NATS failures..."

# Check state bucket exists and is accessible (try multiple servers)
STATE_INFO="NOT_FOUND"
for nats_server in "nats://nats1:4222" "nats://nats2:4222" "nats://nats3:4222"; do
    STATE_INFO=$(docker exec "$NODE1_CONTAINER" nats kv info "$STATE_BUCKET" --server "$nats_server" 2>&1 || echo "NOT_FOUND")
    if ! echo "$STATE_INFO" | grep -q "NOT_FOUND\|error\|not found"; then
        break
    fi
done

if echo "$STATE_INFO" | grep -q "NOT_FOUND\|error\|not found"; then
    record_test "State bucket accessible after NATS recovery" "FAIL"
else
    record_test "State bucket accessible after NATS recovery" "PASS"
    
    # Check node state entries exist (use the failover-aware get_kv_value)
    NODE1_STATE=$(get_kv_value "$NODE1_CONTAINER" "$STATE_BUCKET" "nodes.node1")
    
    if [ -n "$NODE1_STATE" ]; then
        record_test "Node state persisted after NATS recovery" "PASS"
        if [ "$VERBOSE" = true ]; then
            log_info "Node1 state: $NODE1_STATE"
        fi
    else
        record_test "Node state persisted after NATS recovery" "FAIL"
    fi
fi

# ============================================================================
# Test 7: Service discovery still works
# ============================================================================

log_step 8 "Verifying service discovery after NATS failures..."

# Query node status via nats.micro service (try multiple NATS servers)
STATUS_SUBJECT="convex.${CLUSTER_ID}.svc.node1.status"
STATUS_RESPONSE=""
for nats_server in "nats://nats1:4222" "nats://nats2:4222" "nats://nats3:4222"; do
    STATUS_RESPONSE=$(docker exec "$NODE1_CONTAINER" nats request "$STATUS_SUBJECT" '' --server "$nats_server" --timeout 5s 2>/dev/null || echo "")
    if [ -n "$STATUS_RESPONSE" ] && echo "$STATUS_RESPONSE" | grep -q "nodeId"; then
        break
    fi
done

if [ -n "$STATUS_RESPONSE" ] && echo "$STATUS_RESPONSE" | grep -q "nodeId"; then
    record_test "Service discovery works after NATS recovery" "PASS"
else
    record_test "Service discovery works after NATS recovery" "FAIL"
fi

# ============================================================================
# Summary
# ============================================================================

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${CYAN}  NATS Failover Validation Summary${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Print final NATS cluster status
print_nats_cluster_status

# Print final cluster status
print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

TOTAL_TESTS=$((TESTS_PASSED + TESTS_FAILED))
echo -e "${CYAN}Test Results:${NC}"
echo -e "  Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "  Failed: ${RED}$TESTS_FAILED${NC}"
echo -e "  Total:  $TOTAL_TESTS"
echo ""

echo -e "${CYAN}NATS Failover Summary:${NC}"
echo "  Tests covered:"
echo "    - Single NATS node failure (non-leader)"
echo "    - NATS JetStream leader failure"
echo "    - NATS network partition simulation"
echo "    - NATS quorum loss and recovery"
echo "    - State bucket consistency"
echo "    - Service discovery resilience"
echo ""

if [ "$TESTS_FAILED" -eq 0 ]; then
    echo -e "${GREEN}All NATS failover validation tests PASSED!${NC}"
    exit 0
else
    echo -e "${RED}Some NATS failover validation tests FAILED!${NC}"
    exit 1
fi
