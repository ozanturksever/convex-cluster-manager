#!/bin/bash
# test-nats-cluster-all.sh
# Run all NATS cluster failure tests in sequence
#
# Usage:
#   ./test-nats-cluster-all.sh [OPTIONS]
#
# Options:
#   --skip-setup     Skip running test-nats-cluster-setup.sh (assume cluster exists)
#   --keep           Keep containers running after tests
#   --verbose        Show verbose output
#   --help           Show help message

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
SKIP_SETUP=false
KEEP_CONTAINERS=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-setup)
            SKIP_SETUP=true
            shift
            ;;
        --keep)
            KEEP_CONTAINERS=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Run all NATS cluster failure tests in sequence"
            echo ""
            echo "Options:"
            echo "  --skip-setup     Skip running test-nats-cluster-setup.sh (assume cluster exists)"
            echo "  --keep           Keep containers running after tests"
            echo "  --verbose        Show verbose output for each test"
            echo "  --help           Show help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     NATS Cluster Failure Test Suite                        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Track results
declare -a TEST_NAMES
declare -a TEST_RESULTS
declare -a TEST_DURATIONS

# Get current time in seconds
get_time() {
    date +%s
}

# Run a test and track result
run_test() {
    local test_script=$1
    local test_name=$2
    local extra_args=${3:-""}
    
    echo ""
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}  Running: $test_name${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    TEST_NAMES+=("$test_name")
    
    local verbose_flag=""
    if [ "$VERBOSE" = true ]; then
        verbose_flag="--verbose"
    fi
    
    local start_time=$(get_time)
    
    if "$SCRIPT_DIR/$test_script" $verbose_flag $extra_args; then
        TEST_RESULTS+=("PASSED")
    else
        TEST_RESULTS+=("FAILED")
    fi
    
    local end_time=$(get_time)
    local duration=$((end_time - start_time))
    TEST_DURATIONS+=("${duration}s")
    
    # Brief pause between tests to let cluster stabilize
    echo ""
    echo -e "${CYAN}Waiting 10s for cluster to stabilize before next test...${NC}"
    sleep 10
}

SUITE_START_TIME=$(get_time)

# Step 1: Set up the cluster if needed
if [ "$SKIP_SETUP" = false ]; then
    echo -e "${CYAN}Setting up 3-node NATS cluster + 2-node Convex cluster...${NC}"
    echo ""
    
    if "$SCRIPT_DIR/test-nats-cluster-setup.sh" --keep; then
        echo ""
        echo -e "${GREEN}✓ NATS cluster setup complete${NC}"
    else
        echo ""
        echo -e "${RED}✗ NATS cluster setup failed${NC}"
        exit 1
    fi
else
    echo -e "${CYAN}Skipping cluster setup (--skip-setup)${NC}"
    echo ""
fi

# Step 2: Run all NATS failure tests
echo ""
echo -e "${CYAN}Running NATS cluster failure test suite...${NC}"

# Test 1: Single NATS Node Failure
run_test "test-nats-single-node-failure.sh" "NATS Single Node Failure"

# Test 2: NATS JetStream Leader Failure
run_test "test-nats-leader-failure.sh" "NATS JetStream Leader Failure"

# Test 3: NATS Network Partition (Follower)
run_test "test-nats-network-partition.sh" "NATS Network Partition (Follower)" "--partition-time 20"

# Test 4: NATS Network Partition (Leader)
run_test "test-nats-network-partition.sh" "NATS Network Partition (Leader)" "--target-leader --partition-time 20"

SUITE_END_TIME=$(get_time)
SUITE_DURATION=$((SUITE_END_TIME - SUITE_START_TIME))

# Step 3: Print summary
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    Test Suite Summary                      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

PASSED_COUNT=0
FAILED_COUNT=0

for i in "${!TEST_NAMES[@]}"; do
    name="${TEST_NAMES[$i]}"
    result="${TEST_RESULTS[$i]}"
    duration="${TEST_DURATIONS[$i]}"
    
    if [ "$result" = "PASSED" ]; then
        echo -e "  ${GREEN}✓${NC} $name: ${GREEN}$result${NC} ($duration)"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        echo -e "  ${RED}✗${NC} $name: ${RED}$result${NC} ($duration)"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done

TOTAL_TESTS=${#TEST_NAMES[@]}

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  Total:    $TOTAL_TESTS tests"
echo -e "  Passed:   ${GREEN}$PASSED_COUNT${NC}"
echo -e "  Failed:   ${RED}$FAILED_COUNT${NC}"
echo -e "  Duration: ${SUITE_DURATION}s"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Step 4: Cleanup if not keeping containers
if [ "$KEEP_CONTAINERS" = false ]; then
    echo -e "${CYAN}Cleaning up test environment...${NC}"
    "$SCRIPT_DIR/test-nats-cluster-teardown.sh" --keep-output 2>/dev/null || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
else
    echo -e "${YELLOW}Containers kept running (--keep flag)${NC}"
    echo ""
    echo "To clean up manually:"
    echo "  $SCRIPT_DIR/test-nats-cluster-teardown.sh"
fi

# Exit with appropriate code
if [ $FAILED_COUNT -eq 0 ]; then
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║     All NATS cluster failure tests passed!                 ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║     Some NATS cluster failure tests failed.                ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
    exit 1
fi
