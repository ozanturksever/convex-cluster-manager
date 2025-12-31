#!/bin/bash
# lib-nats-cluster.sh
# Common functions for NATS cluster operations in failover tests
#
# This library provides functions to manage a 3-node NATS cluster for testing
# NATS failure scenarios with convex-cluster-manager.
#
# Source this file from test scripts:
#   source "$(dirname "$0")/failover-tests/lib-nats-cluster.sh"

# Ensure lib-failover-common.sh is sourced for base utilities
if [ -z "$PROJECT_ROOT" ]; then
    export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    export CLUSTER_MGR_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
    export PROJECT_ROOT="$(cd "$CLUSTER_MGR_DIR/.." && pwd)"
fi

# Source common library if not already sourced
if ! type log_info &>/dev/null; then
    source "$SCRIPT_DIR/lib-failover-common.sh"
fi

# NATS Cluster Configuration
export NATS_CLUSTER_DIR="${NATS_CLUSTER_DIR:-$CLUSTER_MGR_DIR/scripts/nats-cluster}"
export NATS_COMPOSE_FILE="${NATS_COMPOSE_FILE:-$NATS_CLUSTER_DIR/docker-compose.nats-cluster.yml}"
export NATS_CLUSTER_NETWORK="${NATS_CLUSTER_NETWORK:-convex-nats-cluster}"

# NATS Container Names
export NATS_NODE1_CONTAINER="${NATS_NODE1_CONTAINER:-convex-nats-1}"
export NATS_NODE2_CONTAINER="${NATS_NODE2_CONTAINER:-convex-nats-2}"
export NATS_NODE3_CONTAINER="${NATS_NODE3_CONTAINER:-convex-nats-3}"

# NATS Ports (host ports for accessing each node)
export NATS_NODE1_CLIENT_PORT="${NATS_NODE1_CLIENT_PORT:-4222}"
export NATS_NODE2_CLIENT_PORT="${NATS_NODE2_CLIENT_PORT:-4223}"
export NATS_NODE3_CLIENT_PORT="${NATS_NODE3_CLIENT_PORT:-4224}"

export NATS_NODE1_HTTP_PORT="${NATS_NODE1_HTTP_PORT:-8222}"
export NATS_NODE2_HTTP_PORT="${NATS_NODE2_HTTP_PORT:-8223}"
export NATS_NODE3_HTTP_PORT="${NATS_NODE3_HTTP_PORT:-8224}"

# Internal NATS URLs (for use within Docker network)
export NATS_INTERNAL_URLS="nats://nats1:4222,nats://nats2:4222,nats://nats3:4222"

# ============================================================================
# NATS Cluster Lifecycle Functions
# ============================================================================

# Start the 3-node NATS cluster using docker-compose
start_nats_cluster() {
    local compose_file="${1:-$NATS_COMPOSE_FILE}"
    
    if [ ! -f "$compose_file" ]; then
        log_error "NATS compose file not found: $compose_file"
        return 1
    fi
    
    log_info "Starting NATS cluster from $compose_file"
    
    # Get the directory containing the compose file (needed for config file mounts)
    local compose_dir
    compose_dir=$(dirname "$compose_file")
    
    # Start the cluster from the compose file's directory
    (cd "$compose_dir" && docker-compose -f "$(basename "$compose_file")" up -d)
    
    if [ $? -ne 0 ]; then
        log_error "Failed to start NATS cluster"
        return 1
    fi
    
    # Wait for all nodes to be healthy
    log_info "Waiting for NATS cluster nodes to be healthy..."
    
    local max_wait=60
    local elapsed=0
    
    while [ $elapsed -lt $max_wait ]; do
        local healthy_count=0
        
        if check_nats_node_health "$NATS_NODE1_CONTAINER" "$NATS_NODE1_HTTP_PORT"; then
            healthy_count=$((healthy_count + 1))
        fi
        if check_nats_node_health "$NATS_NODE2_CONTAINER" "$NATS_NODE2_HTTP_PORT"; then
            healthy_count=$((healthy_count + 1))
        fi
        if check_nats_node_health "$NATS_NODE3_CONTAINER" "$NATS_NODE3_HTTP_PORT"; then
            healthy_count=$((healthy_count + 1))
        fi
        
        if [ $healthy_count -eq 3 ]; then
            log_success "All 3 NATS nodes are healthy"
            return 0
        fi
        
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "Timeout waiting for NATS cluster to be healthy"
    return 1
}

# Stop the NATS cluster
stop_nats_cluster() {
    local compose_file="${1:-$NATS_COMPOSE_FILE}"
    local remove_volumes="${2:-false}"
    
    log_info "Stopping NATS cluster..."
    
    # Get the directory containing the compose file
    local compose_dir
    compose_dir=$(dirname "$compose_file")
    
    if [ "$remove_volumes" = true ]; then
        (cd "$compose_dir" && docker-compose -f "$(basename "$compose_file")" down -v)
    else
        (cd "$compose_dir" && docker-compose -f "$(basename "$compose_file")" down)
    fi
    
    log_success "NATS cluster stopped"
}

# ============================================================================
# NATS Node Health Functions
# ============================================================================

# Check if a specific NATS node is healthy
# Args: container_name, http_port (optional, for host access)
check_nats_node_health() {
    local container=$1
    local http_port=$2
    
    # If http_port is provided, use host access; otherwise use docker exec
    if [ -n "$http_port" ]; then
        curl -sf "http://localhost:$http_port/healthz" > /dev/null 2>&1
    else
        docker exec "$container" wget -q --spider "http://localhost:8222/healthz" 2>/dev/null
    fi
}

# Check if a NATS node is running
is_nats_node_running() {
    local container=$1
    docker ps --format '{{.Names}}' | grep -q "^${container}$"
}

# Get NATS node server info
get_nats_node_info() {
    local container=$1
    local http_port=$2
    
    if [ -n "$http_port" ]; then
        curl -sf "http://localhost:$http_port/varz" 2>/dev/null
    else
        docker exec "$container" wget -qO- "http://localhost:8222/varz" 2>/dev/null
    fi
}

# ============================================================================
# NATS Cluster Status Functions
# ============================================================================

# Verify NATS cluster health (requires majority - at least 2 nodes)
verify_nats_cluster_health() {
    local healthy_count=0
    
    if check_nats_node_health "$NATS_NODE1_CONTAINER" "$NATS_NODE1_HTTP_PORT"; then
        healthy_count=$((healthy_count + 1))
    fi
    if check_nats_node_health "$NATS_NODE2_CONTAINER" "$NATS_NODE2_HTTP_PORT"; then
        healthy_count=$((healthy_count + 1))
    fi
    if check_nats_node_health "$NATS_NODE3_CONTAINER" "$NATS_NODE3_HTTP_PORT"; then
        healthy_count=$((healthy_count + 1))
    fi
    
    if [ $healthy_count -ge 2 ]; then
        log_info "NATS cluster healthy: $healthy_count/3 nodes up (quorum maintained)"
        return 0
    else
        log_error "NATS cluster unhealthy: only $healthy_count/3 nodes up (quorum lost)"
        return 1
    fi
}

# Get the number of healthy NATS nodes
get_healthy_nats_node_count() {
    local count=0
    
    if check_nats_node_health "$NATS_NODE1_CONTAINER" "$NATS_NODE1_HTTP_PORT"; then
        count=$((count + 1))
    fi
    if check_nats_node_health "$NATS_NODE2_CONTAINER" "$NATS_NODE2_HTTP_PORT"; then
        count=$((count + 1))
    fi
    if check_nats_node_health "$NATS_NODE3_CONTAINER" "$NATS_NODE3_HTTP_PORT"; then
        count=$((count + 1))
    fi
    
    echo $count
}

# Get cluster route info for a node
get_nats_routes() {
    local container=$1
    local http_port=$2
    
    if [ -n "$http_port" ]; then
        curl -sf "http://localhost:$http_port/routez" 2>/dev/null
    else
        docker exec "$container" wget -qO- "http://localhost:8222/routez" 2>/dev/null
    fi
}

# ============================================================================
# JetStream Leader Functions
# ============================================================================

# Get JetStream info for a node
get_jetstream_info() {
    local container=$1
    local http_port=$2
    
    if [ -n "$http_port" ]; then
        curl -sf "http://localhost:$http_port/jsz" 2>/dev/null
    else
        docker exec "$container" wget -qO- "http://localhost:8222/jsz" 2>/dev/null
    fi
}

# Get the current JetStream meta leader
# Returns the container name of the leader, or empty if not found
get_nats_leader() {
    # Try each node to find the JetStream meta leader
    for container in "$NATS_NODE1_CONTAINER" "$NATS_NODE2_CONTAINER" "$NATS_NODE3_CONTAINER"; do
        local http_port
        case "$container" in
            "$NATS_NODE1_CONTAINER") http_port="$NATS_NODE1_HTTP_PORT" ;;
            "$NATS_NODE2_CONTAINER") http_port="$NATS_NODE2_HTTP_PORT" ;;
            "$NATS_NODE3_CONTAINER") http_port="$NATS_NODE3_HTTP_PORT" ;;
        esac
        
        if ! check_nats_node_health "$container" "$http_port"; then
            continue
        fi
        
        local jsz_info
        jsz_info=$(get_jetstream_info "$container" "$http_port")
        
        if [ -z "$jsz_info" ]; then
            continue
        fi
        
        # Extract the meta leader from JetStream info
        # Note: The leader is in .meta_cluster.leader, not .meta.leader
        local meta_leader
        meta_leader=$(echo "$jsz_info" | jq -r '.meta_cluster.leader // empty' 2>/dev/null)
        
        if [ -n "$meta_leader" ]; then
            # Map server name to container name
            case "$meta_leader" in
                "nats1") echo "$NATS_NODE1_CONTAINER" ;;
                "nats2") echo "$NATS_NODE2_CONTAINER" ;;
                "nats3") echo "$NATS_NODE3_CONTAINER" ;;
                *) echo "$meta_leader" ;;  # Return as-is if unknown
            esac
            return 0
        fi
    done
    
    # No leader found
    echo ""
    return 1
}

# Get JetStream meta leader server name (nats1, nats2, or nats3)
get_nats_leader_name() {
    for container in "$NATS_NODE1_CONTAINER" "$NATS_NODE2_CONTAINER" "$NATS_NODE3_CONTAINER"; do
        local http_port
        case "$container" in
            "$NATS_NODE1_CONTAINER") http_port="$NATS_NODE1_HTTP_PORT" ;;
            "$NATS_NODE2_CONTAINER") http_port="$NATS_NODE2_HTTP_PORT" ;;
            "$NATS_NODE3_CONTAINER") http_port="$NATS_NODE3_HTTP_PORT" ;;
        esac
        
        if ! check_nats_node_health "$container" "$http_port"; then
            continue
        fi
        
        local meta_leader
        meta_leader=$(get_jetstream_info "$container" "$http_port" | jq -r '.meta_cluster.leader // empty' 2>/dev/null)
        
        if [ -n "$meta_leader" ]; then
            echo "$meta_leader"
            return 0
        fi
    done
    
    echo ""
    return 1
}

# Wait for a new JetStream leader to be elected
# Args: excluded_container (optional) - container to exclude (e.g., the killed node)
#       timeout (optional) - max wait time in seconds
wait_for_nats_leader_election() {
    local excluded_container="${1:-}"
    local timeout="${2:-60}"
    local elapsed=0
    
    log_info "Waiting for NATS JetStream leader election..."
    
    while [ $elapsed -lt $timeout ]; do
        local leader
        leader=$(get_nats_leader)
        
        if [ -n "$leader" ]; then
            if [ -z "$excluded_container" ] || [ "$leader" != "$excluded_container" ]; then
                log_success "New JetStream leader elected: $leader"
                return 0
            fi
        fi
        
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "Timeout waiting for JetStream leader election"
    return 1
}

# ============================================================================
# NATS Node Control Functions
# ============================================================================

# Kill (stop) a specific NATS node
kill_nats_node() {
    local container=$1
    
    log_info "Killing NATS node: $container"
    
    docker stop "$container" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        log_success "NATS node $container stopped"
        return 0
    else
        log_error "Failed to stop NATS node $container"
        return 1
    fi
}

# Restart a specific NATS node
restart_nats_node() {
    local container=$1
    
    log_info "Restarting NATS node: $container"
    
    docker start "$container" > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        log_error "Failed to start NATS node $container"
        return 1
    fi
    
    # Wait for the node to be healthy
    local max_wait=30
    local elapsed=0
    local http_port
    
    case "$container" in
        "$NATS_NODE1_CONTAINER") http_port="$NATS_NODE1_HTTP_PORT" ;;
        "$NATS_NODE2_CONTAINER") http_port="$NATS_NODE2_HTTP_PORT" ;;
        "$NATS_NODE3_CONTAINER") http_port="$NATS_NODE3_HTTP_PORT" ;;
    esac
    
    while [ $elapsed -lt $max_wait ]; do
        if check_nats_node_health "$container" "$http_port"; then
            log_success "NATS node $container restarted and healthy"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    log_error "NATS node $container started but not healthy after ${max_wait}s"
    return 1
}

# Force kill a NATS node (docker kill instead of stop)
force_kill_nats_node() {
    local container=$1
    
    log_info "Force killing NATS node: $container"
    
    docker kill "$container" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        log_success "NATS node $container force killed"
        return 0
    else
        log_error "Failed to force kill NATS node $container"
        return 1
    fi
}

# ============================================================================
# Network Partition Functions
# ============================================================================

# Partition a NATS node from the cluster network
partition_nats_node() {
    local container=$1
    local network="${2:-$NATS_CLUSTER_NETWORK}"
    
    log_info "Partitioning NATS node $container from network $network"
    
    docker network disconnect "$network" "$container" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        log_success "NATS node $container disconnected from $network"
        return 0
    else
        log_warn "Failed to disconnect $container from $network (may already be disconnected)"
        return 1
    fi
}

# Heal a network partition by reconnecting a NATS node
heal_nats_partition() {
    local container=$1
    local network="${2:-$NATS_CLUSTER_NETWORK}"
    
    log_info "Healing partition: reconnecting $container to $network"
    
    docker network connect "$network" "$container" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        log_success "NATS node $container reconnected to $network"
        return 0
    else
        log_warn "Failed to reconnect $container to $network (may already be connected)"
        return 1
    fi
}

# Check if a container is connected to a network
is_connected_to_network() {
    local container=$1
    local network=$2
    
    docker network inspect "$network" --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null | grep -q "$container"
}

# ============================================================================
# Utility Functions
# ============================================================================

# Get container name by node number (1, 2, or 3)
get_nats_container_by_number() {
    local node_num=$1
    
    case "$node_num" in
        1) echo "$NATS_NODE1_CONTAINER" ;;
        2) echo "$NATS_NODE2_CONTAINER" ;;
        3) echo "$NATS_NODE3_CONTAINER" ;;
        *) echo "" ;;
    esac
}

# Get HTTP port by container name
get_nats_http_port() {
    local container=$1
    
    case "$container" in
        "$NATS_NODE1_CONTAINER") echo "$NATS_NODE1_HTTP_PORT" ;;
        "$NATS_NODE2_CONTAINER") echo "$NATS_NODE2_HTTP_PORT" ;;
        "$NATS_NODE3_CONTAINER") echo "$NATS_NODE3_HTTP_PORT" ;;
        *) echo "" ;;
    esac
}

# Get client port by container name
get_nats_client_port() {
    local container=$1
    
    case "$container" in
        "$NATS_NODE1_CONTAINER") echo "$NATS_NODE1_CLIENT_PORT" ;;
        "$NATS_NODE2_CONTAINER") echo "$NATS_NODE2_CLIENT_PORT" ;;
        "$NATS_NODE3_CONTAINER") echo "$NATS_NODE3_CLIENT_PORT" ;;
        *) echo "" ;;
    esac
}

# Print NATS cluster status summary
print_nats_cluster_status() {
    echo ""
    echo -e "${CYAN}NATS Cluster Status:${NC}"
    
    for container in "$NATS_NODE1_CONTAINER" "$NATS_NODE2_CONTAINER" "$NATS_NODE3_CONTAINER"; do
        local http_port
        http_port=$(get_nats_http_port "$container")
        
        local status="DOWN"
        local role=""
        
        if is_nats_node_running "$container"; then
            if check_nats_node_health "$container" "$http_port"; then
                status="${GREEN}HEALTHY${NC}"
            else
                status="${YELLOW}UNHEALTHY${NC}"
            fi
        else
            status="${RED}STOPPED${NC}"
        fi
        
        # Check if this is the leader
        local leader
        leader=$(get_nats_leader)
        if [ "$container" = "$leader" ]; then
            role=" (LEADER)"
        fi
        
        echo -e "  $container: $status$role"
    done
    
    local healthy_count
    healthy_count=$(get_healthy_nats_node_count)
    echo ""
    echo -e "  Healthy nodes: $healthy_count/3"
    echo -e "  Quorum: $([ $healthy_count -ge 2 ] && echo "${GREEN}YES${NC}" || echo "${RED}NO${NC}")"
    echo ""
}

# Wait for NATS cluster to be fully healthy (all 3 nodes)
wait_for_full_nats_cluster() {
    local timeout="${1:-60}"
    local elapsed=0
    
    log_info "Waiting for full NATS cluster (all 3 nodes healthy)..."
    
    while [ $elapsed -lt $timeout ]; do
        local count
        count=$(get_healthy_nats_node_count)
        
        if [ "$count" -eq 3 ]; then
            log_success "Full NATS cluster healthy (3/3 nodes)"
            return 0
        fi
        
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "Timeout waiting for full NATS cluster"
    return 1
}

# Wait for NATS cluster quorum (at least 2 nodes)
wait_for_nats_quorum() {
    local timeout="${1:-30}"
    local elapsed=0
    
    log_info "Waiting for NATS cluster quorum (2+ nodes)..."
    
    while [ $elapsed -lt $timeout ]; do
        local count
        count=$(get_healthy_nats_node_count)
        
        if [ "$count" -ge 2 ]; then
            log_success "NATS cluster has quorum ($count/3 nodes)"
            return 0
        fi
        
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "Timeout waiting for NATS cluster quorum"
    return 1
}

# Get a non-leader NATS node container name
get_non_leader_nats_node() {
    local leader
    leader=$(get_nats_leader)
    
    for container in "$NATS_NODE1_CONTAINER" "$NATS_NODE2_CONTAINER" "$NATS_NODE3_CONTAINER"; do
        if [ "$container" != "$leader" ]; then
            local http_port
            http_port=$(get_nats_http_port "$container")
            
            if check_nats_node_health "$container" "$http_port"; then
                echo "$container"
                return 0
            fi
        fi
    done
    
    echo ""
    return 1
}
