#!/bin/bash
# test-nats-cluster-teardown.sh
# Teardown script for NATS cluster failure testing
#
# This script cleans up:
# - 3-node NATS cluster containers
# - 2-node Convex cluster containers
# - Docker networks
# - Test output directory (optional)
#
# Usage:
#   ./test-nats-cluster-teardown.sh [OPTIONS]
#
# Options:
#   --keep-output    Keep the test output directory
#   --verbose        Show verbose output
#   --help           Show help message

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_MGR_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$CLUSTER_MGR_DIR/.." && pwd)"

# Export for sourced scripts
export SCRIPT_DIR CLUSTER_MGR_DIR PROJECT_ROOT

# Source common library for NATS cluster variables
source "$SCRIPT_DIR/failover-tests/lib-nats-cluster.sh" 2>/dev/null || true

# Configuration
TEST_DIR="$CLUSTER_MGR_DIR/test-nats-cluster-output"
NODE1_CONTAINER="${NODE1_CONTAINER:-convex-nats-node1}"
NODE2_CONTAINER="${NODE2_CONTAINER:-convex-nats-node2}"

# Flags
KEEP_OUTPUT=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-output)
            KEEP_OUTPUT=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Teardown NATS cluster + Convex cluster test environment"
            echo ""
            echo "Options:"
            echo "  --keep-output   Keep the test output directory"
            echo "  --verbose       Show verbose output"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     NATS Cluster Test Environment Teardown                 ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Stop Convex node containers
echo -e "${CYAN}Stopping Convex node containers...${NC}"

for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER"; do
    if docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
        if [ "$VERBOSE" = true ]; then
            echo "  Stopping $container..."
        fi
        docker rm -f "$container" 2>/dev/null || true
        echo -e "  ${GREEN}✓${NC} Removed $container"
    else
        if [ "$VERBOSE" = true ]; then
            echo "  $container not found (already removed)"
        fi
    fi
done

# Step 2: Stop NATS cluster
echo -e "${CYAN}Stopping NATS cluster...${NC}"

# Try to use docker-compose first
if [ -f "$NATS_COMPOSE_FILE" ]; then
    docker-compose -f "$NATS_COMPOSE_FILE" down -v 2>/dev/null || true
    echo -e "  ${GREEN}✓${NC} Stopped NATS cluster via docker-compose"
else
    # Fallback to manual container removal
    for container in "convex-nats-1" "convex-nats-2" "convex-nats-3"; do
        if docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
            docker rm -f "$container" 2>/dev/null || true
            if [ "$VERBOSE" = true ]; then
                echo "  Removed $container"
            fi
        fi
    done
    echo -e "  ${GREEN}✓${NC} Stopped NATS containers"
fi

# Step 3: Remove Docker networks
echo -e "${CYAN}Removing Docker networks...${NC}"

for network in "convex-nats-cluster"; do
    if docker network ls --format '{{.Name}}' | grep -q "^${network}$"; then
        docker network rm "$network" 2>/dev/null || true
        echo -e "  ${GREEN}✓${NC} Removed network: $network"
    else
        if [ "$VERBOSE" = true ]; then
            echo "  Network $network not found (already removed)"
        fi
    fi
done

# Step 4: Remove test output directory
if [ "$KEEP_OUTPUT" = false ]; then
    echo -e "${CYAN}Removing test output directory...${NC}"
    if [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
        echo -e "  ${GREEN}✓${NC} Removed $TEST_DIR"
    else
        if [ "$VERBOSE" = true ]; then
            echo "  Directory not found (already removed)"
        fi
    fi
else
    echo -e "${YELLOW}Keeping test output directory: $TEST_DIR${NC}"
fi

# Step 5: Clean up any orphaned volumes
echo -e "${CYAN}Cleaning up Docker volumes...${NC}"

# Remove NATS data volumes if they exist
for volume in "nats-cluster_nats1-data" "nats-cluster_nats2-data" "nats-cluster_nats3-data"; do
    if docker volume ls --format '{{.Name}}' | grep -q "^${volume}$"; then
        docker volume rm "$volume" 2>/dev/null || true
        if [ "$VERBOSE" = true ]; then
            echo "  Removed volume: $volume"
        fi
    fi
done

echo -e "  ${GREEN}✓${NC} Volume cleanup complete"

# Summary
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     Teardown Complete!                                     ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo "Cleaned up:"
echo "  - Convex node containers ($NODE1_CONTAINER, $NODE2_CONTAINER)"
echo "  - NATS cluster containers (convex-nats-1, convex-nats-2, convex-nats-3)"
echo "  - Docker networks"
echo "  - Docker volumes"
if [ "$KEEP_OUTPUT" = false ]; then
    echo "  - Test output directory"
fi

echo ""
echo -e "${GREEN}Done!${NC}"

exit 0
