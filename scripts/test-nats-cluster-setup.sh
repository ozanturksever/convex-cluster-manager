#!/bin/bash
# test-nats-cluster-setup.sh
# Setup script for NATS cluster failure testing
#
# This script sets up:
# - 3-node NATS cluster with JetStream (via docker-compose)
# - 2-node active-passive Convex cluster using convex-cluster-manager
#
# This provides the infrastructure for testing NATS failure scenarios like:
# - Single NATS node failure
# - NATS JetStream leader failure
# - Network partitions
#
# Usage:
#   ./test-nats-cluster-setup.sh [OPTIONS]
#
# Options:
#   --keep           Keep containers running on script exit (for debugging)
#   --skip-build     Skip building binaries (use existing)
#   --verbose        Show verbose output
#   --help           Show help message
#
# After setup, run individual NATS failure test scripts:
#   ./test-nats-single-node-failure.sh
#   ./test-nats-leader-failure.sh
#   ./test-nats-network-partition.sh

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

# Source common libraries
source "$SCRIPT_DIR/failover-tests/lib-failover-common.sh"
source "$SCRIPT_DIR/failover-tests/lib-nats-cluster.sh"

# Configuration
BUNDLER_DIR="$PROJECT_ROOT/convex-bundler"
OPS_DIR="$PROJECT_ROOT/convex-backend-ops"
CONVEX_BACKEND_DIR="$PROJECT_ROOT/convex-backend"
TEST_APP_DIR="$PROJECT_ROOT/test-apps/e2e-cluster-test"
TEST_DIR="$CLUSTER_MGR_DIR/test-nats-cluster-output"
CLUSTER_ID="nats-cluster-test"

# Container names for Convex nodes
NODE1_CONTAINER="convex-nats-node1"
NODE2_CONTAINER="convex-nats-node2"

# Flags
KEEP_CONTAINERS=false
SKIP_BUILD=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep)
            KEEP_CONTAINERS=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Setup 3-node NATS cluster + 2-node Convex cluster for failure testing"
            echo ""
            echo "Options:"
            echo "  --keep         Keep containers running on exit (for debugging)"
            echo "  --skip-build   Skip building binaries (use existing)"
            echo "  --verbose      Show verbose output"
            echo "  --help         Show this help message"
            echo ""
            echo "After setup, run individual NATS failure tests:"
            echo "  ./test-nats-single-node-failure.sh"
            echo "  ./test-nats-leader-failure.sh"
            echo "  ./test-nats-network-partition.sh"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Determine platform for builds
if [[ "$(uname -m)" == "arm64" ]] || [[ "$(uname -m)" == "aarch64" ]]; then
    BUILD_ARCH="arm64"
    PLATFORM="linux-arm64"
else
    BUILD_ARCH="amd64"
    PLATFORM="linux-x64"
fi

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     NATS Cluster + Convex Cluster Setup                    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Platform: ${YELLOW}$PLATFORM${NC}"
echo -e "Test directory: ${YELLOW}$TEST_DIR${NC}"
echo -e "Cluster ID: ${YELLOW}$CLUSTER_ID${NC}"
echo -e "NATS Cluster: ${YELLOW}3 nodes (convex-nats-1, convex-nats-2, convex-nats-3)${NC}"
echo -e "Convex Cluster: ${YELLOW}2 nodes ($NODE1_CONTAINER, $NODE2_CONTAINER)${NC}"
echo ""

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    
    # Stop and remove Convex node containers
    docker rm -f "$NODE1_CONTAINER" "$NODE2_CONTAINER" 2>/dev/null || true
    
    # Stop NATS cluster
    stop_nats_cluster "$NATS_COMPOSE_FILE" true 2>/dev/null || true
    
    if [ "$KEEP_CONTAINERS" = false ]; then
        rm -rf "$TEST_DIR"
    fi
}

# Trap to cleanup on exit (unless --keep is specified)
if [ "$KEEP_CONTAINERS" = false ]; then
    trap cleanup EXIT
fi

# Step 0: Clean up previous test
log_step 0 "Cleaning up previous test output..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

# Also clean up any existing containers
docker rm -f "$NODE1_CONTAINER" "$NODE2_CONTAINER" 2>/dev/null || true
stop_nats_cluster "$NATS_COMPOSE_FILE" true 2>/dev/null || true

log_success "Clean"

# Step 1: Build binaries
if [ "$SKIP_BUILD" = false ]; then
    log_step 1 "Building binaries..."
    
    # Build convex-bundler (for local platform to run pre-deployment)
    log_info "Building convex-bundler..."
    cd "$BUNDLER_DIR"
    go build -o "$TEST_DIR/convex-bundler" .
    log_success "Built convex-bundler"
    
    # Build convex-backend-ops for Linux
    log_info "Building convex-backend-ops for Linux..."
    cd "$OPS_DIR"
    CGO_ENABLED=0 GOOS=linux GOARCH=$BUILD_ARCH go build -o "$TEST_DIR/convex-backend-ops" .
    log_success "Built convex-backend-ops"
    
    # Build convex-cluster-manager for Linux
    log_info "Building convex-cluster-manager for Linux..."
    cd "$CLUSTER_MGR_DIR"
    CGO_ENABLED=0 GOOS=linux GOARCH=$BUILD_ARCH go build -o "$TEST_DIR/convex-cluster-manager" ./cmd/convex-cluster-manager
    log_success "Built convex-cluster-manager"
else
    log_step 1 "Skipping build (using existing binaries)..."
fi

# Step 2: Find the real convex-local-backend binary
log_step 2 "Locating real convex-local-backend binary..."

if [ "$BUILD_ARCH" = "arm64" ]; then
    BACKEND_BINARY_CANDIDATES=(
        "$CONVEX_BACKEND_DIR/build-artifacts/convex-local-backend-linux-arm64"
        "$CONVEX_BACKEND_DIR/target/release/convex-local-backend-linux-arm64"
        "$CONVEX_BACKEND_DIR/target/release/convex-local-backend"
    )
else
    BACKEND_BINARY_CANDIDATES=(
        "$CONVEX_BACKEND_DIR/build-artifacts/convex-local-backend-linux-amd64"
        "$CONVEX_BACKEND_DIR/target/release/convex-local-backend-linux-amd64"
        "$CONVEX_BACKEND_DIR/target/release/convex-local-backend"
    )
fi

BACKEND_BINARY=""
for candidate in "${BACKEND_BINARY_CANDIDATES[@]}"; do
    if [ -f "$candidate" ]; then
        BACKEND_BINARY="$candidate"
        break
    fi
done

if [ -z "$BACKEND_BINARY" ]; then
    log_error "Backend binary not found in any of the expected locations:"
    for candidate in "${BACKEND_BINARY_CANDIDATES[@]}"; do
        log_info "  - $candidate"
    done
    log_info ""
    log_info "To build the backend, run:"
    log_info "  cd $CONVEX_BACKEND_DIR && cargo build --release -p local_backend"
    exit 1
fi

log_info "Using backend: $BACKEND_BINARY"
log_success "Found real convex-local-backend"

# Step 3: Verify test app exists
log_step 3 "Verifying test app..."

if [ ! -d "$TEST_APP_DIR" ]; then
    log_error "Test app not found at $TEST_APP_DIR"
    exit 1
fi

if [ ! -f "$TEST_APP_DIR/convex/schema.ts" ]; then
    log_error "Test app is missing convex/schema.ts"
    exit 1
fi

log_info "Test app: $TEST_APP_DIR"
log_success "Test app verified"

# Step 4: Build pre-deploy Docker image
log_step 4 "Building pre-deploy Docker image..."
log_info "This image has Ubuntu 24.04 with GLIBC 2.39 required by convex-local-backend"

PREDEPLOY_DOCKERFILE="$BUNDLER_DIR/docker/Dockerfile.predeploy"
if [ ! -f "$PREDEPLOY_DOCKERFILE" ]; then
    log_error "Pre-deploy Dockerfile not found at $PREDEPLOY_DOCKERFILE"
    exit 1
fi

docker build -t convex-predeploy:latest -f "$PREDEPLOY_DOCKERFILE" "$BUNDLER_DIR/docker"
log_success "Built pre-deploy Docker image: convex-predeploy:latest"

# Step 5: Create REAL bundle using convex-bundler
log_step 5 "Creating REAL bundle with pre-deployment..."
log_info "This runs 'npx convex deploy' inside Docker to initialize the database"

BUNDLE_DIR="$TEST_DIR/bundle"

"$TEST_DIR/convex-bundler" \
    --app "$TEST_APP_DIR" \
    --output "$BUNDLE_DIR" \
    --backend-binary "$BACKEND_BINARY" \
    --name "NATS Cluster Test" \
    --platform "$PLATFORM"

if [ ! -f "$BUNDLE_DIR/convex.db" ]; then
    log_error "Bundle creation failed - no database file"
    exit 1
fi

if [ ! -f "$BUNDLE_DIR/credentials.json" ]; then
    log_error "Bundle creation failed - no credentials file"
    exit 1
fi

# Extract credentials from the bundle
ADMIN_KEY=$(jq -r '.adminKey' "$BUNDLE_DIR/credentials.json")
INSTANCE_SECRET=$(jq -r '.instanceSecret' "$BUNDLE_DIR/credentials.json")
INSTANCE_NAME=$(echo "$ADMIN_KEY" | cut -d'|' -f1)

log_info "Bundle created at: $BUNDLE_DIR"
log_info "Instance name: $INSTANCE_NAME"
log_info "Admin key: ${ADMIN_KEY:0:30}..."
log_success "Created REAL bundle with pre-deployed app"

# Save credentials for test scripts
cat > "$TEST_DIR/test-config.env" << EOF
# NATS Cluster Test Configuration
# Generated by test-nats-cluster-setup.sh

export ADMIN_KEY="$ADMIN_KEY"
export INSTANCE_SECRET="$INSTANCE_SECRET"
export INSTANCE_NAME="$INSTANCE_NAME"
export CLUSTER_ID="$CLUSTER_ID"
export NODE1_CONTAINER="$NODE1_CONTAINER"
export NODE2_CONTAINER="$NODE2_CONTAINER"
export TEST_DIR="$TEST_DIR"
EOF

log_info "Saved test configuration to $TEST_DIR/test-config.env"

# Step 6: Create selfhost executable
log_step 6 "Creating selfhost executable..."
SELFHOST_PATH="$TEST_DIR/convex-selfhost"

"$TEST_DIR/convex-bundler" selfhost \
    --bundle "$BUNDLE_DIR" \
    --ops-binary "$TEST_DIR/convex-backend-ops" \
    --output "$SELFHOST_PATH" \
    --platform "$PLATFORM" \
    --compression gzip \
    --ops-version "1.0.0-nats-test"

log_success "Created selfhost executable: $(du -h "$SELFHOST_PATH" | cut -f1)"

# Step 7: Build Docker image for cluster nodes
log_step 7 "Building Docker image for cluster nodes..."
DOCKERFILE_PATH="$CLUSTER_MGR_DIR/docker/Dockerfile.cluster-e2e"

if [ ! -f "$DOCKERFILE_PATH" ]; then
    log_error "Dockerfile not found at $DOCKERFILE_PATH"
    exit 1
fi

docker build -t convex-cluster-e2e:latest -f "$DOCKERFILE_PATH" "$CLUSTER_MGR_DIR/docker"
log_success "Built Docker image: convex-cluster-e2e:latest"

# Step 8: Start 3-node NATS cluster
log_step 8 "Starting 3-node NATS cluster..."

if [ ! -f "$NATS_COMPOSE_FILE" ]; then
    log_error "NATS compose file not found at $NATS_COMPOSE_FILE"
    exit 1
fi

start_nats_cluster "$NATS_COMPOSE_FILE"

# Print NATS cluster status
print_nats_cluster_status

# Get NATS leader
NATS_LEADER=$(get_nats_leader)
log_info "NATS JetStream leader: $NATS_LEADER"

log_success "3-node NATS cluster is running"

# Step 9: Start Convex node containers and connect to NATS network
log_step 9 "Starting Convex node containers..."

# Start Node 1
docker run -d \
    --name "$NODE1_CONTAINER" \
    --hostname node1 \
    --privileged \
    --cgroupns=host \
    --tmpfs /run:rw,noexec,nosuid \
    --tmpfs /run/lock:rw,noexec,nosuid \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    convex-cluster-e2e:latest

# Start Node 2
docker run -d \
    --name "$NODE2_CONTAINER" \
    --hostname node2 \
    --privileged \
    --cgroupns=host \
    --tmpfs /run:rw,noexec,nosuid \
    --tmpfs /run/lock:rw,noexec,nosuid \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    convex-cluster-e2e:latest

# Connect Convex nodes to the NATS cluster network
log_info "Connecting Convex nodes to NATS cluster network..."
docker network connect "$NATS_CLUSTER_NETWORK" "$NODE1_CONTAINER"
docker network connect "$NATS_CLUSTER_NETWORK" "$NODE2_CONTAINER"

log_info "Waiting for systemd to initialize..."
wait_for_condition "node1 systemd" 60 \
    "docker exec $NODE1_CONTAINER systemctl is-system-running --wait 2>/dev/null | grep -qE '(running|degraded)'"
wait_for_condition "node2 systemd" 60 \
    "docker exec $NODE2_CONTAINER systemctl is-system-running --wait 2>/dev/null | grep -qE '(running|degraded)'"

log_success "Started Convex node containers (connected to NATS cluster network)"

# Step 10: Copy binaries and bundle to nodes
log_step 10 "Copying binaries and bundle to nodes..."

for NODE in "$NODE1_CONTAINER" "$NODE2_CONTAINER"; do
    # Copy selfhost executable
    docker cp "$SELFHOST_PATH" "$NODE:/tmp/convex-selfhost"
    docker exec "$NODE" chmod +x /tmp/convex-selfhost
    
    # Copy cluster manager
    docker cp "$TEST_DIR/convex-cluster-manager" "$NODE:/usr/local/bin/convex-cluster-manager"
    docker exec "$NODE" chmod +x /usr/local/bin/convex-cluster-manager
done

log_success "Copied binaries to both nodes"

# Step 11: Install selfhost bundle on both nodes
log_step 11 "Installing selfhost bundle on both nodes..."

for NODE in "$NODE1_CONTAINER" "$NODE2_CONTAINER"; do
    log_info "Installing on $NODE..."
    
    # Verify the bundle
    docker exec "$NODE" /tmp/convex-selfhost verify
    
    # Extract the bundle
    docker exec "$NODE" /tmp/convex-selfhost extract --output /tmp/bundle
    
    # Install backend binary
    docker exec "$NODE" cp /tmp/bundle/backend /usr/local/bin/convex-backend
    docker exec "$NODE" chmod +x /usr/local/bin/convex-backend
    
    # Install the pre-initialized database
    docker exec "$NODE" cp /tmp/bundle/convex.db /var/lib/convex/data/convex.db
    docker exec "$NODE" mkdir -p /var/lib/convex/data/storage
    docker exec "$NODE" cp -r /tmp/bundle/storage/. /var/lib/convex/data/storage/ 2>/dev/null || true
    
    # Install manifest and credentials
    docker exec "$NODE" cp /tmp/bundle/manifest.json /var/lib/convex/manifest.json
    docker exec "$NODE" cp /tmp/bundle/credentials.json /etc/convex/credentials.json
    
    # Create convex.env
    docker exec "$NODE" bash -c "cat > /etc/convex/convex.env << EOF
CONVEX_SITE_URL=http://localhost:3210
CONVEX_LOCAL_STORAGE=/var/lib/convex/data/storage
CONVEX_PORT=3210
CONVEX_INSTANCE_NAME=$INSTANCE_NAME
CONVEX_INSTANCE_SECRET=$INSTANCE_SECRET
EOF"

    # Update the systemd service
    docker exec "$NODE" bash -c "cat > /etc/systemd/system/convex-backend.service << EOF
[Unit]
Description=Convex Backend Service
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/convex-backend /var/lib/convex/data/convex.db --port 3210 --instance-name '$INSTANCE_NAME' --instance-secret $INSTANCE_SECRET --local-storage /var/lib/convex/data/storage
Restart=always
RestartSec=5
EnvironmentFile=-/etc/convex/convex.env

[Install]
WantedBy=multi-user.target
EOF"
    
    # Reload systemd
    docker exec "$NODE" systemctl daemon-reload
done

log_success "Installed selfhost bundle on both nodes"

# Step 12: Initialize cluster configuration with MULTIPLE NATS servers
log_step 12 "Initializing cluster configuration (with 3-node NATS cluster)..."

# Note: We configure ALL 3 NATS servers in the cluster config
# This allows convex-cluster-manager to failover between NATS nodes

# Initialize node1 with all 3 NATS servers
docker exec "$NODE1_CONTAINER" /usr/local/bin/convex-cluster-manager init \
    --cluster-id "$CLUSTER_ID" \
    --node-id "node1" \
    --nats "$NATS_INTERNAL_URLS" \
    --config /etc/convex/cluster.json

# Initialize node2 with all 3 NATS servers
docker exec "$NODE2_CONTAINER" /usr/local/bin/convex-cluster-manager init \
    --cluster-id "$CLUSTER_ID" \
    --node-id "node2" \
    --nats "$NATS_INTERNAL_URLS" \
    --config /etc/convex/cluster.json

# Update configs with backend data paths
for NODE in "$NODE1_CONTAINER" "$NODE2_CONTAINER"; do
    docker exec "$NODE" bash -c '
        if jq ".backend.dataPath = \"/var/lib/convex/data/convex.db\" | .wal.replicaPath = \"/var/lib/convex/replica/convex.db\"" /etc/convex/cluster.json > /tmp/cluster.json.new; then
            mv /tmp/cluster.json.new /etc/convex/cluster.json
        fi
    '
done

log_success "Initialized cluster configuration on both nodes"

# Verify NATS configuration in cluster.json
log_info "Verifying NATS server configuration..."
docker exec "$NODE1_CONTAINER" cat /etc/convex/cluster.json | jq '.nats'

# Step 13: Create NATS Object Store bucket for WAL replication
log_step 13 "Creating NATS Object Store bucket..."

BUCKET_NAME="convex-${CLUSTER_ID}-wal"
log_info "Creating bucket: $BUCKET_NAME"

# Create a temporary Go program to create the bucket
cat > "$TEST_DIR/create-bucket.go" << 'EOGO'
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: create-bucket <nats-url> <bucket-name>")
		os.Exit(1)
	}

	natsURL := os.Args[1]
	bucketName := os.Args[2]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Printf("Failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Printf("Failed to create JetStream context: %v\n", err)
		os.Exit(1)
	}

	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Description: "WAL replication bucket for cluster",
		Replicas:    3, // Replicate across all 3 NATS nodes
	})
	if err != nil {
		_, getErr := js.ObjectStore(ctx, bucketName)
		if getErr != nil {
			fmt.Printf("Failed to create/get bucket: create=%v, get=%v\n", err, getErr)
			os.Exit(1)
		}
		fmt.Printf("Bucket %s already exists\n", bucketName)
	} else {
		fmt.Printf("Created bucket: %s (replicated across 3 nodes)\n", bucketName)
	}
}
EOGO

# Build and run the bucket creator for Linux
cd "$TEST_DIR"
go mod init create-bucket 2>/dev/null || true
go get github.com/nats-io/nats.go@latest 2>/dev/null
go get github.com/nats-io/nats.go/jetstream@latest 2>/dev/null
CGO_ENABLED=0 GOOS=linux GOARCH=$BUILD_ARCH go build -o create-bucket create-bucket.go 2>/dev/null

docker cp "$TEST_DIR/create-bucket" "$NODE1_CONTAINER:/tmp/create-bucket"
docker exec "$NODE1_CONTAINER" chmod +x /tmp/create-bucket
docker exec "$NODE1_CONTAINER" /tmp/create-bucket "nats://nats1:4222" "$BUCKET_NAME"

log_success "Created NATS Object Store bucket: $BUCKET_NAME (replicated x3)"

# Step 14: Start cluster manager daemon on both nodes
log_step 14 "Starting cluster manager daemon on both nodes..."

log_info "Starting daemon on node1..."
docker exec -d "$NODE1_CONTAINER" bash -c '/usr/local/bin/convex-cluster-manager daemon --config /etc/convex/cluster.json >> /var/log/cluster-manager.log 2>&1'

sleep 5

log_info "Starting daemon on node2..."
docker exec -d "$NODE2_CONTAINER" bash -c '/usr/local/bin/convex-cluster-manager daemon --config /etc/convex/cluster.json >> /var/log/cluster-manager.log 2>&1'

log_info "Waiting for leader election (10 seconds)..."
sleep 10

if [ "$VERBOSE" = true ]; then
    log_info "Node1 daemon log:"
    docker exec "$NODE1_CONTAINER" cat /var/log/cluster-manager.log 2>/dev/null | tail -20 || true
    log_info "Node2 daemon log:"
    docker exec "$NODE2_CONTAINER" cat /var/log/cluster-manager.log 2>/dev/null | tail -20 || true
fi

log_success "Started cluster manager daemons"

# Step 15: Validate leader election
log_step 15 "Validating leader election..."

NODE1_STATUS=$(docker exec "$NODE1_CONTAINER" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "STATUS_ERROR")
NODE2_STATUS=$(docker exec "$NODE2_CONTAINER" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 || echo "STATUS_ERROR")

log_info "Node1 status:"
echo "$NODE1_STATUS" | head -20

log_info "Node2 status:"
echo "$NODE2_STATUS" | head -20

# Determine which node is PRIMARY
if echo "$NODE1_STATUS" | grep -q "PRIMARY"; then
    log_success "Node1 is PRIMARY (leader)"
    PRIMARY_NODE="$NODE1_CONTAINER"
    PASSIVE_NODE="$NODE2_CONTAINER"
elif echo "$NODE2_STATUS" | grep -q "PRIMARY"; then
    log_success "Node2 is PRIMARY (leader)"
    PRIMARY_NODE="$NODE2_CONTAINER"
    PASSIVE_NODE="$NODE1_CONTAINER"
else
    log_error "No PRIMARY node detected!"
    log_info "Node1 daemon log:"
    docker exec "$NODE1_CONTAINER" cat /var/log/cluster-manager.log 2>/dev/null | tail -30 || true
    log_info "Node2 daemon log:"
    docker exec "$NODE2_CONTAINER" cat /var/log/cluster-manager.log 2>/dev/null | tail -30 || true
    PRIMARY_NODE=""
    PASSIVE_NODE=""
fi

if echo "$NODE1_STATUS" | grep -q "PASSIVE" || echo "$NODE2_STATUS" | grep -q "PASSIVE"; then
    log_success "Passive node detected"
fi

# Step 16: Wait for backend to be ready
log_step 16 "Waiting for Convex backend to be ready..."

if [ -n "$PRIMARY_NODE" ]; then
    wait_for_condition "Backend health check" 60 \
        "docker exec $PRIMARY_NODE curl -sf http://localhost:3210/version > /dev/null 2>&1"
    
    BACKEND_VERSION=$(docker exec "$PRIMARY_NODE" curl -s http://localhost:3210/version 2>/dev/null || echo "unknown")
    log_info "Backend version: $BACKEND_VERSION"
    
    # Verify WAL mode
    WAL_MODE=$(docker exec "$PRIMARY_NODE" sqlite3 /var/lib/convex/data/convex.db "PRAGMA journal_mode;" 2>/dev/null || echo "unknown")
    if [ "$WAL_MODE" = "wal" ]; then
        log_success "Database is in WAL mode"
    else
        log_warn "Database is NOT in WAL mode (got: $WAL_MODE)"
    fi
    
    log_success "Convex backend is ready"
else
    log_warn "Skipping backend check (no primary detected)"
fi

# Step 17: Test Convex functions
log_step 17 "Testing Convex functions via HTTP API..."

if [ -n "$PRIMARY_NODE" ]; then
    TEST_MESSAGE="NATS-Cluster-Test-$(date +%s)"
    log_info "Adding test message: $TEST_MESSAGE"
    
    MUTATION_RESPONSE=$(docker exec "$PRIMARY_NODE" curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "Authorization: Convex $ADMIN_KEY" \
        -d "{\"path\": \"messages:add\", \"args\": {\"content\": \"$TEST_MESSAGE\", \"author\": \"nats-test\"}, \"format\": \"json\"}" \
        "http://localhost:3210/api/mutation" 2>&1)
    
    if echo "$MUTATION_RESPONSE" | grep -q '"status":"success"'; then
        log_success "Convex mutation successful!"
        
        # Query to verify
        QUERY_RESPONSE=$(docker exec "$PRIMARY_NODE" curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "Authorization: Convex $ADMIN_KEY" \
            -d '{"path": "messages:list", "args": {}, "format": "json"}' \
            "http://localhost:3210/api/query" 2>&1)
        
        if echo "$QUERY_RESPONSE" | grep -q "$TEST_MESSAGE"; then
            log_success "Data verified on primary node!"
        else
            log_warn "Test message not found in query response"
        fi
    else
        log_error "Mutation failed!"
        log_info "Response: $MUTATION_RESPONSE"
    fi
else
    log_warn "Skipping Convex function calls (no primary detected)"
fi

# Step 18: Summary
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     NATS Cluster Setup Complete!                           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${CYAN}Infrastructure Summary:${NC}"
echo ""
print_nats_cluster_status

echo -e "${CYAN}Convex Cluster Status:${NC}"
echo ""
echo "Node1 ($NODE1_CONTAINER):"
docker exec "$NODE1_CONTAINER" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 | head -10 || echo "  (daemon not running)"
echo ""
echo "Node2 ($NODE2_CONTAINER):"
docker exec "$NODE2_CONTAINER" /usr/local/bin/convex-cluster-manager status --config /etc/convex/cluster.json 2>&1 | head -10 || echo "  (daemon not running)"

echo ""
echo -e "${CYAN}Test Configuration:${NC}"
echo "  Config file: $TEST_DIR/test-config.env"
echo "  Admin key: ${ADMIN_KEY:0:30}..."
echo "  NATS URLs: $NATS_INTERNAL_URLS"
echo ""

if [ "$KEEP_CONTAINERS" = true ]; then
    echo -e "${YELLOW}Containers kept running (--keep flag).${NC}"
    echo ""
    echo "To run NATS failure tests:"
    echo "  $SCRIPT_DIR/test-nats-single-node-failure.sh"
    echo "  $SCRIPT_DIR/test-nats-leader-failure.sh"
    echo "  $SCRIPT_DIR/test-nats-network-partition.sh"
    echo "  $SCRIPT_DIR/test-nats-cluster-all.sh"
    echo ""
    echo "To inspect containers:"
    echo "  docker exec -it $NODE1_CONTAINER bash"
    echo "  docker exec -it $NODE2_CONTAINER bash"
    echo "  docker exec -it convex-nats-1 sh"
    echo ""
    echo "To clean up manually:"
    echo "  $SCRIPT_DIR/test-nats-cluster-teardown.sh"
else
    echo "To keep containers for testing, run with --keep flag"
fi

echo ""
echo -e "${GREEN}Done! Ready for NATS failure testing.${NC}"

exit 0
