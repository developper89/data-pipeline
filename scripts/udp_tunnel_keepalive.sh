# ==============================================================================
# 3. udp_tunnel_keepalive.sh - Resilient tunnel with auto-restart
# ==============================================================================
#!/bin/bash
# udp_tunnel_keepalive.sh - Resilient UDP tunnel with auto-restart

PID_FILE="./scripts/udp_tunnel_pids.txt"
LOG_FILE="./scripts/tunnel.log"
HEALTH_CHECK_INTERVAL=30
MAX_RETRIES=5

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to safely kill a process by PID (handles both local and remote)
safe_kill() {
    local pid=$1
    local process_name=$2
    
    if [[ "$process_name" == "SERVER_RELAY" ]]; then
        # Handle remote process
        if ssh pres "kill -0 $pid" 2>/dev/null; then
            ssh pres "kill -TERM $pid" 2>/dev/null
            sleep 2
            if ssh pres "kill -0 $pid" 2>/dev/null; then
                ssh pres "kill -KILL $pid" 2>/dev/null
                log "Force killed remote $process_name (PID: $pid)"
            else
                log "Gracefully stopped remote $process_name (PID: $pid)"
            fi
        fi
    else
        # Handle local process
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null
            sleep 2
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid" 2>/dev/null
                log "Force killed $process_name (PID: $pid)"
            else
                log "Gracefully stopped $process_name (PID: $pid)"
            fi
        fi
    fi
}

# Function to safely kill processes using specific ports (Docker-aware)
safe_port_cleanup() {
    local port=$1
    log "Cleaning up port $port..."
    
    local pids=$(sudo lsof -ti :$port 2>/dev/null | while read pid; do
        local process_info=$(ps -p $pid -o comm= 2>/dev/null)
        if [[ ! "$process_info" =~ (docker|com.docker|Docker) ]]; then
            echo $pid
        fi
    done)
    
    if [ -n "$pids" ]; then
        for pid in $pids; do
            local process_name=$(ps -p $pid -o comm= 2>/dev/null)
            log "Found non-Docker process on port $port: $process_name (PID: $pid)"
            safe_kill "$pid" "$process_name"
        done
    fi
}

# Function to test tunnel connectivity
test_tunnel() {
    # Test if we can establish SSH connection
    if ! ssh -o ConnectTimeout=5 -o BatchMode=yes pres exit >/dev/null 2>&1; then
        log "SSH connection test failed"
        return 1
    fi
    
    # Test if local port is responding
    if ! nc -z localhost 5684 >/dev/null 2>&1; then
        log "Local port 5684 test failed"
        return 1
    fi
    
    # Test if remote port forward is working
    if ! ssh -o ConnectTimeout=5 -o BatchMode=yes pres "nc -z localhost 5684" >/dev/null 2>&1; then
        log "Remote port forward test failed"
        return 1
    fi
    
    # Test if remote socat process is actually running
    if ! ssh pres "pgrep -f 'socat.*5683'" >/dev/null 2>&1; then
        log "Remote socat process not found"
        return 1
    fi
    
    # Test if all tracked processes are actually running
    if [ -f "$PID_FILE" ]; then
        while read -r pid_line; do
            if [ -n "$pid_line" ]; then
                process_name=$(echo "$pid_line" | cut -d: -f1)
                pid=$(echo "$pid_line" | cut -d: -f2)
                
                case "$process_name" in
                    "SERVER_RELAY")
                        if ! ssh pres "kill -0 $pid" 2>/dev/null; then
                            log "Remote process $process_name (PID: $pid) is not running"
                            return 1
                        fi
                        ;;
                    *)
                        if ! kill -0 "$pid" 2>/dev/null; then
                            log "Local process $process_name (PID: $pid) is not running"
                            return 1
                        fi
                        ;;
                esac
            fi
        done < "$PID_FILE"
    fi
    
    return 0
}

# Function to start tunnel components
start_tunnel() {
    log "Starting tunnel components..."
    
    # Clean up any existing processes
    cleanup_tunnel
    
    # Start SSH tunnel with keep-alive (disable control master for this tunnel)
    ssh -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
        -o ControlMaster=no -o ControlPath=none \
        -R 5684:localhost:5684 pres -N &
    SSH_PID=$!
    echo "SSH_TUNNEL:$SSH_PID" > "$PID_FILE"
    log "SSH Tunnel started (PID: $SSH_PID)"
    
    sleep 3
    
    # Verify SSH tunnel is working by checking process and port
    if ! kill -0 "$SSH_PID" 2>/dev/null; then
        log "ERROR: SSH tunnel process failed to start"
        return 1
    fi
    
    # Additional verification - check if the port forward is actually working
    if ! ssh -o ConnectTimeout=5 -o BatchMode=yes pres "nc -z localhost 5684" >/dev/null 2>&1; then
        log "ERROR: SSH tunnel port forward not working"
        return 1
    fi
    
    # Start server-side UDP relay and get the actual remote PID
    SERVER_RELAY_PID=$(ssh pres "nohup sudo socat UDP4-LISTEN:5683,fork TCP:localhost:5684 >/dev/null 2>&1 & echo \$!")
    echo "SERVER_RELAY:$SERVER_RELAY_PID" >> "$PID_FILE"
    log "Server relay started (remote PID: $SERVER_RELAY_PID)"
    
    sleep 2
    
    # Start local TCP→UDP relay (suppress expected connection errors)
    socat TCP4-LISTEN:5684,fork UDP:localhost:5683 2>/dev/null &
    LOCAL_RELAY_PID=$!
    echo "LOCAL_RELAY:$LOCAL_RELAY_PID" >> "$PID_FILE"
    log "Local relay started (PID: $LOCAL_RELAY_PID) - Connection refused errors during CoAP server restarts are suppressed"
    
    sleep 2
    
    # Test connectivity
    if test_tunnel; then
        log "✅ Tunnel established successfully"
        return 0
    else
        log "❌ Tunnel test failed"
        return 1
    fi
}

# Function to cleanup tunnel
cleanup_tunnel() {
    if [ -f "$PID_FILE" ]; then
        log "Cleaning up tunnel processes..."
        while read -r pid_line; do
            if [ -n "$pid_line" ]; then
                process_name=$(echo "$pid_line" | cut -d: -f1)
                pid=$(echo "$pid_line" | cut -d: -f2)
                safe_kill "$pid" "$process_name"
            fi
        done < "$PID_FILE"
        rm "$PID_FILE"
    fi
    
    # Kill caffeinate process if it exists
    if [ -n "$CAFFEINATE_PID" ] && kill -0 "$CAFFEINATE_PID" 2>/dev/null; then
        kill "$CAFFEINATE_PID" 2>/dev/null
        log "Stopped caffeinate process (PID: $CAFFEINATE_PID)"
    fi
    
    safe_port_cleanup 5683
    safe_port_cleanup 5684
    pkill -f "socat.*5683" 2>/dev/null
    pkill -f "socat.*5684" 2>/dev/null
    
    # Clean up any orphaned SSH tunnel processes
    pkill -f "ssh.*pres.*5684" 2>/dev/null || true
    
    # Cancel any existing port forwards through SSH control master
    ssh -O cancel -R 5684:localhost:5684 pres 2>/dev/null || true
    
    # Clean up remote socat processes
    ssh pres "sudo pkill -f 'socat.*5683'" 2>/dev/null || true
}

# Function to monitor and restart tunnel
monitor_tunnel() {
    local retry_count=0
    
    while true; do
        if test_tunnel; then
            log "Tunnel health check: OK"
            retry_count=0
        else
            log "Tunnel health check: FAILED"
            retry_count=$((retry_count + 1))
            
            if [ $retry_count -le $MAX_RETRIES ]; then
                log "Attempting to restart tunnel (attempt $retry_count/$MAX_RETRIES)"
                if start_tunnel; then
                    log "Tunnel restarted successfully"
                    retry_count=0
                else
                    log "Tunnel restart failed"
                    sleep $((retry_count * 10))  # Exponential backoff
                fi
            else
                log "Max retries exceeded. Sleeping for 5 minutes before trying again..."
                sleep 300
                retry_count=0
            fi
        fi
        
        sleep $HEALTH_CHECK_INTERVAL
    done
}

# Handle signals for graceful shutdown
trap 'log "Received shutdown signal"; cleanup_tunnel; exit 0' SIGTERM SIGINT

# Prevent system sleep while script is running
caffeinate -i -s &
CAFFEINATE_PID=$!
log "Started caffeinate to prevent system sleep (PID: $CAFFEINATE_PID)"

# Verify Docker is available
if ! docker info >/dev/null 2>&1; then
    log "ERROR: Docker is not available"
    exit 1
fi

log "Starting UDP tunnel with auto-restart capability..."

# Initial tunnel setup
if start_tunnel; then
    log "Initial tunnel setup successful. Starting monitoring..."
    monitor_tunnel
else
    log "Initial tunnel setup failed"
    cleanup_tunnel
    exit 1
fi