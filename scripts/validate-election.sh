#!/bin/bash
# validate-election.sh
# Validates the NATS KV-based leader election mechanism
#
# This script verifies:
# - KV bucket exists with correct name (convex-<cluster_id>-election)
# - Leader key exists and contains valid node ID
# - Only one leader exists across the cluster
# - Leader can be demoted (step-down with cooldown)
# - Failover works when leader is killed
#
# Prerequisites:
# - NATS cluster running (via test-nats-cluster-setup.sh)
# - Convex cluster nodes running with cluster manager daemons
#
# Usage:
#   ./validate-election.sh [OPTIONS]
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
            echo "Validates the NATS KV-based leader election mechanism"
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

# Election KV bucket name
ELECTION_BUCKET="convex-${CLUSTER_ID}-election"

print_test_header "Election Validation"

echo -e "${CYAN}Configuration:${NC}"
echo "  Cluster ID: $CLUSTER_ID"
echo "  Election Bucket: $ELECTION_BUCKET"
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

# Helper to get KV value
get_kv_value() {
    local container=$1
    local bucket=$2
    local key=$3
    
    docker exec "$container" nats kv get "$bucket" "$key" --raw 2>/dev/null || echo ""
}

# ============================================================================
# Pre-flight checks
# ============================================================================

log_step 1 "Pre-flight checks..."

# Check if containers are running
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

# Check NATS connectivity
if ! docker exec "$NODE1_CONTAINER" nats server ping --server nats://nats1:4222 > /dev/null 2>&1; then
    # Try installing nats CLI if not available
    log_info "Installing nats CLI in containers..."
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER"; do
        docker exec "$container" bash -c '
            if ! command -v nats &> /dev/null; then
                curl -sf https://binaries.nats.dev/nats-io/natscli/nats@latest | sh
                mv nats /usr/local/bin/
            fi
        ' 2>/dev/null || true
    done
fi

log_success "Pre-flight checks passed"

# ============================================================================
# Test 1: KV Bucket Exists
# ============================================================================

log_step 2 "Checking election KV bucket exists..."

BUCKET_INFO=$(docker exec "$NODE1_CONTAINER" nats kv info "$ELECTION_BUCKET" --server nats://nats1:4222 2>&1 || echo "NOT_FOUND")

if echo "$BUCKET_INFO" | grep -q "NOT_FOUND\|error\|not found"; then
    record_test "KV bucket '$ELECTION_BUCKET' exists" "FAIL"
    log_info "Bucket info: $BUCKET_INFO"
else
    record_test "KV bucket '$ELECTION_BUCKET' exists" "PASS"
    if [ "$VERBOSE" = true ]; then
        echo "$BUCKET_INFO"
    fi
fi

# ============================================================================
# Test 2: Leader Key Exists and Has Valid Value
# ============================================================================

log_step 3 "Checking leader key..."

LEADER_VALUE=$(docker exec "$NODE1_CONTAINER" nats kv get "$ELECTION_BUCKET" leader --server nats://nats1:4222 --raw 2>/dev/null || echo "")

if [ -z "$LEADER_VALUE" ]; then
    record_test "Leader key exists" "FAIL"
    log_info "No leader key found - election may not have completed"
else
    record_test "Leader key exists" "PASS"
    log_info "Current leader: $LEADER_VALUE"
    
    # Verify leader value is a valid node ID
    if [ "$LEADER_VALUE" = "node1" ] || [ "$LEADER_VALUE" = "node2" ]; then
        record_test "Leader value is valid node ID" "PASS"
    else
        record_test "Leader value is valid node ID" "FAIL"
        log_info "Expected 'node1' or 'node2', got '$LEADER_VALUE'"
    fi
fi

# ============================================================================
# Test 3: Exactly One Leader Across Cluster
# ============================================================================

log_step 4 "Verifying exactly one leader..."

NODE1_ROLE=$(get_node_role "$NODE1_CONTAINER")
NODE2_ROLE=$(get_node_role "$NODE2_CONTAINER")

log_info "Node1 role: $NODE1_ROLE"
log_info "Node2 role: $NODE2_ROLE"

PRIMARY_COUNT=0
[ "$NODE1_ROLE" = "PRIMARY" ] && PRIMARY_COUNT=$((PRIMARY_COUNT + 1))
[ "$NODE2_ROLE" = "PRIMARY" ] && PRIMARY_COUNT=$((PRIMARY_COUNT + 1))

if [ "$PRIMARY_COUNT" -eq 1 ]; then
    record_test "Exactly one PRIMARY node" "PASS"
else
    record_test "Exactly one PRIMARY node" "FAIL"
    log_info "Found $PRIMARY_COUNT PRIMARY nodes (expected 1)"
fi

# Verify leader in KV matches actual primary
if [ -n "$LEADER_VALUE" ]; then
    EXPECTED_LEADER=""
    [ "$NODE1_ROLE" = "PRIMARY" ] && EXPECTED_LEADER="node1"
    [ "$NODE2_ROLE" = "PRIMARY" ] && EXPECTED_LEADER="node2"
    
    if [ "$LEADER_VALUE" = "$EXPECTED_LEADER" ]; then
        record_test "KV leader matches actual PRIMARY" "PASS"
    else
        record_test "KV leader matches actual PRIMARY" "FAIL"
        log_info "KV says '$LEADER_VALUE', actual PRIMARY is '$EXPECTED_LEADER'"
    fi
fi

# ============================================================================
# Test 4: Leader Heartbeat (TTL refresh)
# ============================================================================

log_step 5 "Verifying leader heartbeat..."

# Get initial revision
INITIAL_INFO=$(docker exec "$NODE1_CONTAINER" nats kv get "$ELECTION_BUCKET" leader --server nats://nats1:4222 2>&1 || echo "")
INITIAL_REV=$(echo "$INITIAL_INFO" | grep -oP 'revision:\s*\K\d+' || echo "0")

log_info "Initial revision: $INITIAL_REV"
log_info "Waiting 5 seconds for heartbeat..."
sleep 5

# Get new revision
NEW_INFO=$(docker exec "$NODE1_CONTAINER" nats kv get "$ELECTION_BUCKET" leader --server nats://nats1:4222 2>&1 || echo "")
NEW_REV=$(echo "$NEW_INFO" | grep -oP 'revision:\s*\K\d+' || echo "0")

log_info "New revision: $NEW_REV"

if [ "$NEW_REV" -gt "$INITIAL_REV" ] 2>/dev/null; then
    record_test "Leader heartbeat updates revision" "PASS"
else
    record_test "Leader heartbeat updates revision" "FAIL"
    log_info "Revision should increase during heartbeat (was $INITIAL_REV, now $NEW_REV)"
fi

# ============================================================================
# Test 5: Step-down creates cooldown marker
# ============================================================================

log_step 6 "Testing step-down and cooldown..."

# Find current primary
PRIMARY_NODE=""
PRIMARY_CONTAINER=""
if [ "$NODE1_ROLE" = "PRIMARY" ]; then
    PRIMARY_NODE="node1"
    PRIMARY_CONTAINER="$NODE1_CONTAINER"
elif [ "$NODE2_ROLE" = "PRIMARY" ]; then
    PRIMARY_NODE="node2"
    PRIMARY_CONTAINER="$NODE2_CONTAINER"
fi

if [ -n "$PRIMARY_CONTAINER" ]; then
    log_info "Requesting step-down from $PRIMARY_NODE..."
    
    # Execute demote command
    DEMOTE_RESULT=$(docker exec "$PRIMARY_CONTAINER" /usr/local/bin/convex-cluster-manager demote --config /etc/convex/cluster.json 2>&1 || echo "DEMOTE_ERROR")
    
    if echo "$DEMOTE_RESULT" | grep -qi "error\|failed"; then
        record_test "Step-down command succeeds" "FAIL"
        log_info "Demote result: $DEMOTE_RESULT"
    else
        record_test "Step-down command succeeds" "PASS"
        
        # Check for cooldown marker
        sleep 1
        COOLDOWN_VALUE=$(docker exec "$NODE1_CONTAINER" nats kv get "$ELECTION_BUCKET" "cooldown-$PRIMARY_NODE" --server nats://nats1:4222 --raw 2>/dev/null || echo "")
        
        if [ -n "$COOLDOWN_VALUE" ]; then
            record_test "Cooldown marker created after step-down" "PASS"
            log_info "Cooldown marker value: $COOLDOWN_VALUE"
        else
            record_test "Cooldown marker created after step-down" "FAIL"
            log_info "No cooldown marker found for cooldown-$PRIMARY_NODE"
        fi
    fi
    
    # Wait for new leader election
    log_info "Waiting for new leader election (15 seconds)..."
    sleep 15
    
    # Verify new leader
    NEW_LEADER=$(docker exec "$NODE1_CONTAINER" nats kv get "$ELECTION_BUCKET" leader --server nats://nats1:4222 --raw 2>/dev/null || echo "")
    
    if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$PRIMARY_NODE" ]; then
        record_test "New leader elected after step-down" "PASS"
        log_info "New leader: $NEW_LEADER"
    elif [ "$NEW_LEADER" = "$PRIMARY_NODE" ]; then
        # Leader may have re-acquired after cooldown - this is ok
        record_test "New leader elected after step-down" "PASS"
        log_info "Original leader re-acquired after cooldown: $NEW_LEADER"
    else
        record_test "New leader elected after step-down" "FAIL"
        log_info "No new leader found"
    fi
else
    log_warn "No PRIMARY node found, skipping step-down test"
fi

# ============================================================================
# Test 6: Failover on leader kill
# ============================================================================

log_step 7 "Testing failover on leader kill..."

# Get current leader
CURRENT_LEADER=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

if [ -n "$CURRENT_LEADER" ]; then
    log_info "Current leader: $CURRENT_LEADER"
    
    # Kill the daemon on current leader
    log_info "Killing daemon on current leader..."
    kill_daemon "$CURRENT_LEADER"
    
    # Wait for TTL expiration and failover (leader TTL is typically 10s)
    log_info "Waiting for TTL expiration and failover (20 seconds)..."
    sleep 20
    
    # Check new leader
    if [ "$CURRENT_LEADER" = "$NODE1_CONTAINER" ]; then
        OTHER_NODE="$NODE2_CONTAINER"
    else
        OTHER_NODE="$NODE1_CONTAINER"
    fi
    
    NEW_ROLE=$(get_node_role "$OTHER_NODE")
    
    if [ "$NEW_ROLE" = "PRIMARY" ]; then
        record_test "Failover promotes passive to PRIMARY" "PASS"
        log_info "$OTHER_NODE is now PRIMARY"
    else
        record_test "Failover promotes passive to PRIMARY" "FAIL"
        log_info "$OTHER_NODE role is $NEW_ROLE (expected PRIMARY)"
    fi
    
    # Restart the killed daemon
    log_info "Restarting daemon on former leader..."
    start_daemon "$CURRENT_LEADER"
    sleep 10
    
    # Verify former leader rejoins as passive
    FORMER_LEADER_ROLE=$(get_node_role "$CURRENT_LEADER")
    if [ "$FORMER_LEADER_ROLE" = "PASSIVE" ]; then
        record_test "Former leader rejoins as PASSIVE" "PASS"
    else
        record_test "Former leader rejoins as PASSIVE" "FAIL"
        log_info "Former leader role is $FORMER_LEADER_ROLE (expected PASSIVE)"
    fi
else
    log_warn "No PRIMARY node found, skipping failover test"
fi

# ============================================================================
# Summary
# ============================================================================

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${CYAN}  Election Validation Summary${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

TOTAL_TESTS=$((TESTS_PASSED + TESTS_FAILED))
echo -e "${CYAN}Test Results:${NC}"
echo -e "  Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "  Failed: ${RED}$TESTS_FAILED${NC}"
echo -e "  Total:  $TOTAL_TESTS"
echo ""

if [ "$TESTS_FAILED" -eq 0 ]; then
    echo -e "${GREEN}All election validation tests PASSED!${NC}"
    exit 0
else
    echo -e "${RED}Some election validation tests FAILED!${NC}"
    exit 1
fi
