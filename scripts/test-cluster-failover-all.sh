#!/bin/bash
# test-cluster-failover-all.sh
# Run all cluster failover tests in sequence
#
# Usage:
#   ./test-cluster-failover-all.sh [OPTIONS]
#
# Options:
#   --skip-setup     Skip running test-cluster-e2e.sh (assume cluster exists)
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
            echo "Run all cluster failover tests in sequence"
            echo ""
            echo "Options:"
            echo "  --skip-setup     Skip running test-cluster-e2e.sh (assume cluster exists)"
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
echo -e "${BLUE}║     Convex Cluster Failover Test Suite                     ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Track results
declare -a TEST_NAMES
declare -a TEST_RESULTS

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
    
    if "$SCRIPT_DIR/$test_script" $verbose_flag $extra_args; then
        TEST_RESULTS+=("PASSED")
    else
        TEST_RESULTS+=("FAILED")
    fi
    
    # Brief pause between tests
    sleep 5
}

# Step 1: Set up the cluster if needed
if [ "$SKIP_SETUP" = false ]; then
    echo -e "${CYAN}Setting up test cluster...${NC}"
    echo ""
    
    if "$SCRIPT_DIR/test-cluster-e2e.sh" --keep; then
        echo ""
        echo -e "${GREEN}✓ Test cluster setup complete${NC}"
    else
        echo ""
        echo -e "${RED}✗ Test cluster setup failed${NC}"
        exit 1
    fi
else
    echo -e "${CYAN}Skipping cluster setup (--skip-setup)${NC}"
    echo ""
fi

# Step 2: Run all failover tests
echo ""
echo -e "${CYAN}Running failover test suite...${NC}"

# Test 1: Basic Failover
run_test "test-cluster-failover-basic.sh" "Basic Failover"

# Test 2: Graceful Failover
run_test "test-cluster-failover-graceful.sh" "Graceful Failover"

# Test 3: Rapid Failover (reduced iterations for full suite)
run_test "test-cluster-failover-rapid.sh" "Rapid Failover" "--iterations 2"

# Test 4: Data Integrity
run_test "test-cluster-failover-data-integrity.sh" "Data Integrity"

# Test 5: Network Partition
run_test "test-cluster-failover-network.sh" "Network Partition"

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
    
    if [ "$result" = "PASSED" ]; then
        echo -e "  ${GREEN}✓${NC} $name: ${GREEN}$result${NC}"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        echo -e "  ${RED}✗${NC} $name: ${RED}$result${NC}"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done

TOTAL_TESTS=${#TEST_NAMES[@]}

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  Total:  $TOTAL_TESTS tests"
echo -e "  Passed: ${GREEN}$PASSED_COUNT${NC}"
echo -e "  Failed: ${RED}$FAILED_COUNT${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Step 4: Cleanup if not keeping containers
if [ "$KEEP_CONTAINERS" = false ]; then
    echo -e "${CYAN}Cleaning up test containers...${NC}"
    docker rm -f convex-e2e-nats convex-e2e-node1 convex-e2e-node2 2>/dev/null || true
    docker network rm convex-e2e-network 2>/dev/null || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
else
    echo -e "${YELLOW}Containers kept running (--keep flag)${NC}"
    echo ""
    echo "To clean up manually:"
    echo "  docker rm -f convex-e2e-nats convex-e2e-node1 convex-e2e-node2"
    echo "  docker network rm convex-e2e-network"
fi

# Exit with appropriate code
if [ $FAILED_COUNT -eq 0 ]; then
    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi
