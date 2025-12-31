#!/bin/bash
# test-cluster-failover-rapid.sh
# Rapid failover test: Multiple consecutive failovers to stress test
#
# Prerequisites:
#   - Run scripts/test-cluster-e2e.sh --keep first to set up the cluster
#   - Or have a running cluster with the expected container names
#
# Usage:
#   ./test-cluster-failover-rapid.sh [OPTIONS]
#
# Options:
#   --nats-container NAME    NATS container name (default: convex-e2e-nats)
#   --node1-container NAME   Node1 container name (default: convex-e2e-node1)
#   --node2-container NAME   Node2 container name (default: convex-e2e-node2)
#   --iterations N           Number of failover cycles (default: 3)
#   --wait-time SECONDS      Wait time between failovers (default: 20)
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
ITERATIONS=3
WAIT_TIME=20
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
        --iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        --wait-time)
            WAIT_TIME="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Rapid failover test: Multiple consecutive failovers to stress test"
            echo ""
            echo "Options:"
            echo "  --nats-container NAME    NATS container name (default: convex-e2e-nats)"
            echo "  --node1-container NAME   Node1 container name (default: convex-e2e-node1)"
            echo "  --node2-container NAME   Node2 container name (default: convex-e2e-node2)"
            echo "  --iterations N           Number of failover cycles (default: 3)"
            echo "  --wait-time SECONDS      Wait time between failovers (default: 20)"
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
TEST_NAME="Rapid Failover Test ($ITERATIONS iterations)"
TEST_PASSED=true
SUCCESSFUL_FAILOVERS=0
FAILED_FAILOVERS=0

print_test_header "$TEST_NAME"

echo -e "${CYAN}Test Configuration:${NC}"
echo "  Iterations: $ITERATIONS"
echo "  Wait time between failovers: ${WAIT_TIME}s"
echo ""

# Step 1: Verify containers are running
log_step 1 "Verifying cluster containers are running..."

if ! check_required_containers "$NATS_CONTAINER" "$NODE1_CONTAINER" "$NODE2_CONTAINER"; then
    log_error "Required containers not running. Run 'scripts/test-cluster-e2e.sh --keep' first."
    exit 1
fi

log_success "All required containers are running"

# Step 2: Ensure both daemons are running
log_step 2 "Ensuring cluster is stable..."

# Kill any existing daemons to get a clean start
kill_daemon "$NODE1_CONTAINER" 2>/dev/null || true
kill_daemon "$NODE2_CONTAINER" 2>/dev/null || true
sleep 3

# Start daemons fresh
start_daemon "$NODE1_CONTAINER"
start_daemon "$NODE2_CONTAINER"

# Wait for leader election with retries
log_info "Waiting for leader election..."
MAX_RETRIES=6
RETRY_DELAY=10
PRIMARY_NODE=""
PASSIVE_NODE=""

for retry in $(seq 1 $MAX_RETRIES); do
    sleep $RETRY_DELAY
    PRIMARY_NODE=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    PASSIVE_NODE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    
    if [ -n "$PRIMARY_NODE" ]; then
        break
    fi
    
    log_info "Retry $retry/$MAX_RETRIES: No PRIMARY yet, waiting..."
done

if [ -z "$PRIMARY_NODE" ]; then
    log_error "No PRIMARY node detected after $MAX_RETRIES retries"
    log_info "Node1 status:"
    get_node_status "$NODE1_CONTAINER" || true
    log_info "Node2 status:"
    get_node_status "$NODE2_CONTAINER" || true
    exit 1
fi

log_success "Cluster is stable"
log_info "Initial PRIMARY: $PRIMARY_NODE"
log_info "Initial PASSIVE: $PASSIVE_NODE"

# Wait for backend to be ready
if ! wait_for_backend "$PRIMARY_NODE" 60; then
    log_error "Backend not healthy on primary"
    exit 1
fi

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# Step 3: Run rapid failover iterations
log_step 3 "Starting rapid failover test..."

for i in $(seq 1 $ITERATIONS); do
    echo ""
    echo -e "${YELLOW}━━━ Failover Iteration $i of $ITERATIONS ━━━${NC}"
    
    # Get current primary and passive
    CURRENT_PRIMARY=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    CURRENT_PASSIVE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
    
    if [ -z "$CURRENT_PRIMARY" ]; then
        log_error "Iteration $i: No PRIMARY node detected before failover"
        FAILED_FAILOVERS=$((FAILED_FAILOVERS + 1))
        continue
    fi
    
    log_info "Iteration $i: Current PRIMARY=$CURRENT_PRIMARY, PASSIVE=$CURRENT_PASSIVE"
    
    # Kill the primary daemon
    log_info "Iteration $i: Killing daemon on $CURRENT_PRIMARY..."
    kill_daemon "$CURRENT_PRIMARY"
    
    # Wait for failover
    log_info "Iteration $i: Waiting ${WAIT_TIME}s for failover..."
    sleep $WAIT_TIME
    
    # Check if failover occurred
    if [ -n "$CURRENT_PASSIVE" ]; then
        NEW_PRIMARY_ROLE=$(get_node_role "$CURRENT_PASSIVE")
        
        if [ "$NEW_PRIMARY_ROLE" = "PRIMARY" ]; then
            log_success "Iteration $i: Failover SUCCESS - $CURRENT_PASSIVE is now PRIMARY"
            SUCCESSFUL_FAILOVERS=$((SUCCESSFUL_FAILOVERS + 1))
        else
            log_error "Iteration $i: Failover FAILED - $CURRENT_PASSIVE is $NEW_PRIMARY_ROLE"
            FAILED_FAILOVERS=$((FAILED_FAILOVERS + 1))
            TEST_PASSED=false
        fi
    else
        log_error "Iteration $i: No passive node was available for failover"
        FAILED_FAILOVERS=$((FAILED_FAILOVERS + 1))
        TEST_PASSED=false
    fi
    
    # Restart the killed daemon so it becomes passive
    log_info "Iteration $i: Restarting daemon on $CURRENT_PRIMARY..."
    start_daemon "$CURRENT_PRIMARY"
    
    # Wait for it to stabilize
    sleep 10
    
    if [ "$VERBOSE" = true ]; then
        print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"
    fi
    
    # Brief pause before next iteration
    if [ $i -lt $ITERATIONS ]; then
        log_info "Waiting 5s before next iteration..."
        sleep 5
    fi
done

# Step 4: Final validation
log_step 4 "Final cluster validation..."

FINAL_PRIMARY=$(get_primary_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")
FINAL_PASSIVE=$(get_passive_node "$NODE1_CONTAINER" "$NODE2_CONTAINER")

print_cluster_status "$NODE1_CONTAINER" "$NODE2_CONTAINER"

if [ -n "$FINAL_PRIMARY" ] && [ -n "$FINAL_PASSIVE" ]; then
    log_success "Cluster is in healthy state after rapid failovers"
else
    log_error "Cluster is not in healthy state"
    TEST_PASSED=false
fi

# Verify backend is working
if [ -n "$FINAL_PRIMARY" ]; then
    if wait_for_backend "$FINAL_PRIMARY" 30; then
        log_success "Backend is healthy on final primary"
    else
        log_error "Backend not healthy on final primary"
        TEST_PASSED=false
    fi
fi

# Print summary
echo ""
echo -e "${CYAN}━━━ Rapid Failover Summary ━━━${NC}"
echo "  Total iterations:     $ITERATIONS"
echo "  Successful failovers: $SUCCESSFUL_FAILOVERS"
echo "  Failed failovers:     $FAILED_FAILOVERS"
echo "  Success rate:         $(( (SUCCESSFUL_FAILOVERS * 100) / ITERATIONS ))%"
echo ""

# Print test result
if [ "$TEST_PASSED" = true ] && [ $SUCCESSFUL_FAILOVERS -eq $ITERATIONS ]; then
    print_test_result "$TEST_NAME" "PASSED"
    exit 0
else
    print_test_result "$TEST_NAME" "FAILED"
    exit 1
fi
