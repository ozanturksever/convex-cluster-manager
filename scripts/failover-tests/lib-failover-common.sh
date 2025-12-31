#!/bin/bash
# lib-failover-common.sh
# Common functions and setup for cluster failover tests
#
# Source this file from individual test scripts:
#   source "$(dirname "$0")/failover-tests/lib-failover-common.sh"

set -e

# Colors for output
export RED='\033[0;31m'
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export CYAN='\033[0;36m'
export NC='\033[0m' # No Color

# Get script directory and project root
if [ -z "$PROJECT_ROOT" ]; then
    export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    export CLUSTER_MGR_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
    export PROJECT_ROOT="$(cd "$CLUSTER_MGR_DIR/.." && pwd)"
fi

# Default configuration
export BUNDLER_DIR="${BUNDLER_DIR:-$PROJECT_ROOT/convex-bundler}"
export OPS_DIR="${OPS_DIR:-$PROJECT_ROOT/convex-backend-ops}"
export CLUSTER_MGR_DIR="${CLUSTER_MGR_DIR:-$CLUSTER_MGR_DIR}"
export CONVEX_BACKEND_DIR="${CONVEX_BACKEND_DIR:-$PROJECT_ROOT/convex-backend}"
export TEST_APP_DIR="${TEST_APP_DIR:-$PROJECT_ROOT/test-apps/e2e-cluster-test}"

# Determine platform for builds
if [[ "$(uname -m)" == "arm64" ]] || [[ "$(uname -m)" == "aarch64" ]]; then
    export BUILD_ARCH="arm64"
    export PLATFORM="linux-arm64"
else
    export BUILD_ARCH="amd64"
    export PLATFORM="linux-x64"
fi

# Helper functions
log_step() {
    echo -e "\n${BLUE}Step $1: $2${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

log_error() {
    echo -e "${RED}✗ $1${NC}"
}

log_info() {
    echo -e "${CYAN}  $1${NC}"
}

log_warn() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

wait_for_condition() {
    local description=$1
    local timeout=$2
    local check_cmd=$3
    
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if eval "$check_cmd" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    log_error "Timeout waiting for: $description"
    return 1
}

# Get the current primary node
get_primary_node() {
    local node1_container=$1
    local node2_container=$2
    
    local node1_status=$(docker exec "$node1_container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "ERROR")
    local node2_status=$(docker exec "$node2_container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "ERROR")
    
    if echo "$node1_status" | grep -q "PRIMARY"; then
        echo "$node1_container"
    elif echo "$node2_status" | grep -q "PRIMARY"; then
        echo "$node2_container"
    else
        echo ""
    fi
}

# Get the current passive node
get_passive_node() {
    local node1_container=$1
    local node2_container=$2
    
    local node1_status=$(docker exec "$node1_container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "ERROR")
    local node2_status=$(docker exec "$node2_container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "ERROR")
    
    if echo "$node1_status" | grep -q "PASSIVE"; then
        echo "$node1_container"
    elif echo "$node2_status" | grep -q "PASSIVE"; then
        echo "$node2_container"
    else
        echo ""
    fi
}

# Wait for a node to become PRIMARY
wait_for_primary() {
    local container=$1
    local timeout=${2:-30}
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        local status=$(docker exec "$container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "ERROR")
        if echo "$status" | grep -q "PRIMARY"; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    return 1
}

# Wait for a node to become PASSIVE
wait_for_passive() {
    local container=$1
    local timeout=${2:-30}
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        local status=$(docker exec "$container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "ERROR")
        if echo "$status" | grep -q "PASSIVE"; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    return 1
}

# Kill the cluster daemon on a node
kill_daemon() {
    local container=$1
    docker exec "$container" pkill -f "convex-cluster-manager daemon" 2>/dev/null || true
}

# Start the cluster daemon on a node
start_daemon() {
    local container=$1
    docker exec -d "$container" bash -c '/usr/local/bin/convex-cluster-manager daemon --config /etc/convex/cluster.json >> /var/log/cluster-manager.log 2>&1'
}

# Get node status
get_node_status() {
    local container=$1
    docker exec "$container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "STATUS_ERROR"
}

# Get node role (PRIMARY, PASSIVE, or UNKNOWN)
get_node_role() {
    local container=$1
    local status=$(get_node_status "$container")
    
    if echo "$status" | grep -q "PRIMARY"; then
        echo "PRIMARY"
    elif echo "$status" | grep -q "PASSIVE"; then
        echo "PASSIVE"
    else
        echo "UNKNOWN"
    fi
}

# Call a Convex mutation
call_mutation() {
    local container=$1
    local admin_key=$2
    local path=$3
    local args=$4
    
    docker exec "$container" curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "Authorization: Convex $admin_key" \
        -d "{\"path\": \"$path\", \"args\": $args, \"format\": \"json\"}" \
        "http://localhost:3210/api/mutation" 2>&1
}

# Call a Convex query
call_query() {
    local container=$1
    local admin_key=$2
    local path=$3
    local args=${4:-"{}"}
    
    docker exec "$container" curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "Authorization: Convex $admin_key" \
        -d "{\"path\": \"$path\", \"args\": $args, \"format\": \"json\"}" \
        "http://localhost:3210/api/query" 2>&1
}

# Check if backend is healthy
check_backend_health() {
    local container=$1
    docker exec "$container" curl -sf http://localhost:3210/version > /dev/null 2>&1
}

# Wait for backend to be healthy
wait_for_backend() {
    local container=$1
    local timeout=${2:-60}
    wait_for_condition "Backend health on $container" $timeout "docker exec $container curl -sf http://localhost:3210/version > /dev/null 2>&1"
}

# Print node statuses
print_cluster_status() {
    local node1_container=$1
    local node2_container=$2
    
    echo ""
    echo -e "${CYAN}Current Cluster Status:${NC}"
    echo "Node1 ($node1_container):"
    docker exec "$node1_container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 | head -10 || echo "  (daemon not running)"
    echo ""
    echo "Node2 ($node2_container):"
    docker exec "$node2_container" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 | head -10 || echo "  (daemon not running)"
    echo ""
}

# Check if required containers are running
check_required_containers() {
    local nats_container=$1
    local node1_container=$2
    local node2_container=$3
    
    if ! docker ps --format '{{.Names}}' | grep -q "^${nats_container}$"; then
        log_error "NATS container '$nats_container' is not running"
        return 1
    fi
    
    if ! docker ps --format '{{.Names}}' | grep -q "^${node1_container}$"; then
        log_error "Node1 container '$node1_container' is not running"
        return 1
    fi
    
    if ! docker ps --format '{{.Names}}' | grep -q "^${node2_container}$"; then
        log_error "Node2 container '$node2_container' is not running"
        return 1
    fi
    
    return 0
}

# Disconnect a container from the network (simulate network partition)
disconnect_from_network() {
    local container=$1
    local network=$2
    docker network disconnect "$network" "$container" 2>/dev/null || true
}

# Reconnect a container to the network
reconnect_to_network() {
    local container=$1
    local network=$2
    docker network connect "$network" "$container" 2>/dev/null || true
}

# Get message count from the messages table
get_message_count() {
    local container=$1
    local admin_key=$2
    
    local response=$(call_query "$container" "$admin_key" "messages:count")
    echo "$response" | grep -oP '"value":\s*\K\d+' || echo "0"
}

# Add a test message
add_test_message() {
    local container=$1
    local admin_key=$2
    local content=$3
    local author=${4:-"failover-test"}
    
    call_mutation "$container" "$admin_key" "messages:add" "{\"content\": \"$content\", \"author\": \"$author\"}"
}

# Verify a message exists
verify_message_exists() {
    local container=$1
    local admin_key=$2
    local content=$3
    
    local response=$(call_query "$container" "$admin_key" "messages:list")
    echo "$response" | grep -q "$content"
}

# Print test header
print_test_header() {
    local test_name=$1
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}  $test_name${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

# Print test result
print_test_result() {
    local test_name=$1
    local result=$2  # "PASSED" or "FAILED"
    
    echo ""
    echo -e "${BLUE}============================================${NC}"
    if [ "$result" = "PASSED" ]; then
        echo -e "${GREEN}  TEST $result: $test_name${NC}"
    else
        echo -e "${RED}  TEST $result: $test_name${NC}"
    fi
    echo -e "${BLUE}============================================${NC}"
    echo ""
}
