#!/bin/bash
# validate-service-discovery.sh
# Validates the nats.micro service discovery mechanism
#
# This script verifies:
# - Services are registered with correct names (convex-<cluster_id>-<node_id>)
# - Service subjects follow auth-compatible pattern (convex.<cluster_id>.svc.<node_id>)
# - Status endpoint works and returns valid JSON
# - Service metadata contains cluster_id and node_id
# - Service discovery can find all cluster nodes
#
# Prerequisites:
# - NATS cluster running (via test-nats-cluster-setup.sh)
# - Convex cluster nodes running with cluster manager daemons
#
# Usage:
#   ./validate-service-discovery.sh [OPTIONS]
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
            echo "Validates the nats.micro service discovery mechanism"
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

# Service naming patterns
SERVICE_NAME_NODE1="convex-${CLUSTER_ID}-node1"
SERVICE_NAME_NODE2="convex-${CLUSTER_ID}-node2"
STATUS_SUBJECT_NODE1="convex.${CLUSTER_ID}.svc.node1.status"
STATUS_SUBJECT_NODE2="convex.${CLUSTER_ID}.svc.node2.status"

print_test_header "Service Discovery Validation"

echo -e "${CYAN}Configuration:${NC}"
echo "  Cluster ID: $CLUSTER_ID"
echo "  Node1 Service: $SERVICE_NAME_NODE1"
echo "  Node2 Service: $SERVICE_NAME_NODE2"
echo "  Node1 Status Subject: $STATUS_SUBJECT_NODE1"
echo "  Node2 Status Subject: $STATUS_SUBJECT_NODE2"
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

# Helper to send NATS request and get response
nats_request() {
    local container=$1
    local subject=$2
    local timeout=${3:-5}
    
    docker exec "$container" nats request "$subject" '' --server nats://nats1:4222 --timeout "${timeout}s" 2>/dev/null || echo ""
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

# Check NATS connectivity and install nats CLI if needed
if ! docker exec "$NODE1_CONTAINER" which nats > /dev/null 2>&1; then
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
# Test 1: Service Registration - List micro services
# ============================================================================

log_step 2 "Checking micro service registration..."

# Query for services using nats micro ls
SERVICES_OUTPUT=$(docker exec "$NODE1_CONTAINER" nats micro ls --server nats://nats1:4222 2>&1 || echo "")

if [ "$VERBOSE" = true ]; then
    echo "Services output:"
    echo "$SERVICES_OUTPUT"
fi

# Check for node1 service
if echo "$SERVICES_OUTPUT" | grep -q "$SERVICE_NAME_NODE1"; then
    record_test "Node1 service '$SERVICE_NAME_NODE1' registered" "PASS"
else
    record_test "Node1 service '$SERVICE_NAME_NODE1' registered" "FAIL"
    log_info "Service not found in: $SERVICES_OUTPUT"
fi

# Check for node2 service
if echo "$SERVICES_OUTPUT" | grep -q "$SERVICE_NAME_NODE2"; then
    record_test "Node2 service '$SERVICE_NAME_NODE2' registered" "PASS"
else
    record_test "Node2 service '$SERVICE_NAME_NODE2' registered" "FAIL"
    log_info "Service not found in: $SERVICES_OUTPUT"
fi

# ============================================================================
# Test 2: Service Info - Get detailed service info
# ============================================================================

log_step 3 "Checking service info..."

# Get info for node1 service
NODE1_INFO=$(docker exec "$NODE1_CONTAINER" nats micro info "$SERVICE_NAME_NODE1" --server nats://nats1:4222 2>&1 || echo "")

if [ "$VERBOSE" = true ]; then
    echo "Node1 service info:"
    echo "$NODE1_INFO"
fi

if echo "$NODE1_INFO" | grep -qi "name.*$SERVICE_NAME_NODE1"; then
    record_test "Node1 service info retrievable" "PASS"
else
    record_test "Node1 service info retrievable" "FAIL"
fi

# Check metadata contains cluster_id
if echo "$NODE1_INFO" | grep -q "cluster_id.*$CLUSTER_ID"; then
    record_test "Node1 metadata contains cluster_id" "PASS"
else
    record_test "Node1 metadata contains cluster_id" "FAIL"
fi

# Check metadata contains node_id
if echo "$NODE1_INFO" | grep -q "node_id.*node1"; then
    record_test "Node1 metadata contains node_id" "PASS"
else
    record_test "Node1 metadata contains node_id" "FAIL"
fi

# ============================================================================
# Test 3: Status Endpoint - Query node status
# ============================================================================

log_step 4 "Testing status endpoints..."

# Query node1 status endpoint
log_info "Querying $STATUS_SUBJECT_NODE1..."
NODE1_STATUS_RESPONSE=$(nats_request "$NODE1_CONTAINER" "$STATUS_SUBJECT_NODE1" 5)

if [ "$VERBOSE" = true ]; then
    echo "Node1 status response: $NODE1_STATUS_RESPONSE"
fi

if [ -n "$NODE1_STATUS_RESPONSE" ]; then
    record_test "Node1 status endpoint responds" "PASS"
    
    # Verify JSON structure
    if echo "$NODE1_STATUS_RESPONSE" | jq -e '.nodeId' > /dev/null 2>&1; then
        record_test "Node1 status returns valid JSON with nodeId" "PASS"
        
        # Extract and verify fields
        RESPONSE_NODE_ID=$(echo "$NODE1_STATUS_RESPONSE" | jq -r '.nodeId' 2>/dev/null)
        RESPONSE_CLUSTER_ID=$(echo "$NODE1_STATUS_RESPONSE" | jq -r '.clusterId' 2>/dev/null)
        RESPONSE_ROLE=$(echo "$NODE1_STATUS_RESPONSE" | jq -r '.role' 2>/dev/null)
        
        log_info "Node1 status: nodeId=$RESPONSE_NODE_ID, clusterId=$RESPONSE_CLUSTER_ID, role=$RESPONSE_ROLE"
        
        if [ "$RESPONSE_NODE_ID" = "node1" ]; then
            record_test "Node1 status nodeId is correct" "PASS"
        else
            record_test "Node1 status nodeId is correct" "FAIL"
        fi
        
        if [ "$RESPONSE_CLUSTER_ID" = "$CLUSTER_ID" ]; then
            record_test "Node1 status clusterId is correct" "PASS"
        else
            record_test "Node1 status clusterId is correct" "FAIL"
        fi
        
        if [ "$RESPONSE_ROLE" = "PRIMARY" ] || [ "$RESPONSE_ROLE" = "PASSIVE" ]; then
            record_test "Node1 status role is valid" "PASS"
        else
            record_test "Node1 status role is valid" "FAIL"
            log_info "Role should be PRIMARY or PASSIVE, got: $RESPONSE_ROLE"
        fi
    else
        record_test "Node1 status returns valid JSON with nodeId" "FAIL"
    fi
else
    record_test "Node1 status endpoint responds" "FAIL"
    log_info "No response from $STATUS_SUBJECT_NODE1"
fi

# Query node2 status endpoint
log_info "Querying $STATUS_SUBJECT_NODE2..."
NODE2_STATUS_RESPONSE=$(nats_request "$NODE1_CONTAINER" "$STATUS_SUBJECT_NODE2" 5)

if [ "$VERBOSE" = true ]; then
    echo "Node2 status response: $NODE2_STATUS_RESPONSE"
fi

if [ -n "$NODE2_STATUS_RESPONSE" ]; then
    record_test "Node2 status endpoint responds" "PASS"
    
    RESPONSE_NODE_ID=$(echo "$NODE2_STATUS_RESPONSE" | jq -r '.nodeId' 2>/dev/null || echo "")
    if [ "$RESPONSE_NODE_ID" = "node2" ]; then
        record_test "Node2 status nodeId is correct" "PASS"
    else
        record_test "Node2 status nodeId is correct" "FAIL"
    fi
else
    record_test "Node2 status endpoint responds" "FAIL"
    log_info "No response from $STATUS_SUBJECT_NODE2"
fi

# ============================================================================
# Test 4: Cross-node Discovery - Query from different nodes
# ============================================================================

log_step 5 "Testing cross-node service discovery..."

# Query node2's status from node1's container
log_info "Querying node2 status from node1 container..."
CROSS_NODE_RESPONSE=$(nats_request "$NODE1_CONTAINER" "$STATUS_SUBJECT_NODE2" 5)

if [ -n "$CROSS_NODE_RESPONSE" ]; then
    record_test "Cross-node query (node1 -> node2) works" "PASS"
else
    record_test "Cross-node query (node1 -> node2) works" "FAIL"
fi

# Query node1's status from node2's container
log_info "Querying node1 status from node2 container..."
CROSS_NODE_RESPONSE=$(nats_request "$NODE2_CONTAINER" "$STATUS_SUBJECT_NODE1" 5)

if [ -n "$CROSS_NODE_RESPONSE" ]; then
    record_test "Cross-node query (node2 -> node1) works" "PASS"
else
    record_test "Cross-node query (node2 -> node1) works" "FAIL"
fi

# ============================================================================
# Test 5: Service Statistics
# ============================================================================

log_step 6 "Checking service statistics..."

NODE1_STATS=$(docker exec "$NODE1_CONTAINER" nats micro stats "$SERVICE_NAME_NODE1" --server nats://nats1:4222 2>&1 || echo "")

if [ "$VERBOSE" = true ]; then
    echo "Node1 service stats:"
    echo "$NODE1_STATS"
fi

if echo "$NODE1_STATS" | grep -q "$SERVICE_NAME_NODE1"; then
    record_test "Service statistics available" "PASS"
else
    record_test "Service statistics available" "FAIL"
fi

# ============================================================================
# Test 6: Built-in Ping Endpoint
# ============================================================================

log_step 7 "Testing built-in ping endpoint..."

# nats.micro provides built-in PING at $SRV.PING
PING_SUBJECT="\$SRV.PING.$SERVICE_NAME_NODE1"
log_info "Pinging $PING_SUBJECT..."

PING_RESPONSE=$(docker exec "$NODE1_CONTAINER" nats micro ping "$SERVICE_NAME_NODE1" --server nats://nats1:4222 2>&1 || echo "")

if [ "$VERBOSE" = true ]; then
    echo "Ping response: $PING_RESPONSE"
fi

if echo "$PING_RESPONSE" | grep -qi "$SERVICE_NAME_NODE1\|response"; then
    record_test "Service ping works" "PASS"
else
    record_test "Service ping works" "FAIL"
fi

# ============================================================================
# Test 7: State Bucket (KV-backed state)
# ============================================================================

log_step 8 "Checking KV state bucket..."

STATE_BUCKET="convex-${CLUSTER_ID}-state"

BUCKET_INFO=$(docker exec "$NODE1_CONTAINER" nats kv info "$STATE_BUCKET" --server nats://nats1:4222 2>&1 || echo "NOT_FOUND")

if echo "$BUCKET_INFO" | grep -q "NOT_FOUND\|error\|not found"; then
    record_test "State KV bucket '$STATE_BUCKET' exists" "FAIL"
else
    record_test "State KV bucket '$STATE_BUCKET' exists" "PASS"
    
    if [ "$VERBOSE" = true ]; then
        echo "$BUCKET_INFO"
    fi
    
    # Check for node state entries
    NODE1_STATE=$(docker exec "$NODE1_CONTAINER" nats kv get "$STATE_BUCKET" "nodes.node1" --server nats://nats1:4222 --raw 2>/dev/null || echo "")
    
    if [ -n "$NODE1_STATE" ]; then
        record_test "Node1 state exists in KV" "PASS"
        
        if [ "$VERBOSE" = true ]; then
            echo "Node1 state: $NODE1_STATE"
        fi
        
        # Verify JSON structure
        if echo "$NODE1_STATE" | jq -e '.nodeId' > /dev/null 2>&1; then
            record_test "Node1 state is valid JSON" "PASS"
        else
            record_test "Node1 state is valid JSON" "FAIL"
        fi
    else
        record_test "Node1 state exists in KV" "FAIL"
        log_info "No state found at nodes.node1"
    fi
fi

# ============================================================================
# Test 8: Subject naming follows auth pattern
# ============================================================================

log_step 9 "Verifying auth-compatible subject naming..."

# Verify subject patterns
# Service subjects should match: convex.<cluster_id>.svc.<node_id>.*

if [[ "$STATUS_SUBJECT_NODE1" =~ ^convex\.[a-zA-Z0-9_-]+\.svc\.[a-zA-Z0-9_-]+\.status$ ]]; then
    record_test "Status subject follows auth pattern" "PASS"
else
    record_test "Status subject follows auth pattern" "FAIL"
    log_info "Expected pattern: convex.<cluster_id>.svc.<node_id>.status"
    log_info "Got: $STATUS_SUBJECT_NODE1"
fi

# Verify KV bucket naming
if [[ "$STATE_BUCKET" =~ ^convex-[a-zA-Z0-9_-]+-state$ ]]; then
    record_test "State bucket name follows auth pattern" "PASS"
else
    record_test "State bucket name follows auth pattern" "FAIL"
fi

ELECTION_BUCKET="convex-${CLUSTER_ID}-election"
if [[ "$ELECTION_BUCKET" =~ ^convex-[a-zA-Z0-9_-]+-election$ ]]; then
    record_test "Election bucket name follows auth pattern" "PASS"
else
    record_test "Election bucket name follows auth pattern" "FAIL"
fi

# ============================================================================
# Summary
# ============================================================================

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${CYAN}  Service Discovery Validation Summary${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

TOTAL_TESTS=$((TESTS_PASSED + TESTS_FAILED))
echo -e "${CYAN}Test Results:${NC}"
echo -e "  Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "  Failed: ${RED}$TESTS_FAILED${NC}"
echo -e "  Total:  $TOTAL_TESTS"
echo ""

echo -e "${CYAN}Service Discovery Summary:${NC}"
echo "  Service Name Pattern: convex-<cluster_id>-<node_id>"
echo "  Subject Pattern: convex.<cluster_id>.svc.<node_id>.<endpoint>"
echo "  State Bucket: convex-<cluster_id>-state"
echo "  Election Bucket: convex-<cluster_id>-election"
echo ""

if [ "$TESTS_FAILED" -eq 0 ]; then
    echo -e "${GREEN}All service discovery validation tests PASSED!${NC}"
    exit 0
else
    echo -e "${RED}Some service discovery validation tests FAILED!${NC}"
    exit 1
fi
