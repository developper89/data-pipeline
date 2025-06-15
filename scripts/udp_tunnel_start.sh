#!/bin/bash
# udp_tunnel_start2.sh

PID_FILE="./scripts/udp_tunnel_pids.txt"

echo "Starting UDP Tunnel for CoAP Traffic..."

# Function to safely kill a process by PID
safe_kill() {
    local pid=$1
    local process_name=$2
    
    if kill -0 "$pid" 2>/dev/null; then
        # First try graceful termination
        kill -TERM "$pid" 2>/dev/null
        sleep 2
        
        # Check if process is still running
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null
            echo "Force killed $process_name (PID: $pid)"
        else
            echo "Gracefully stopped $process_name (PID: $pid)"
        fi
    fi
}

# Function to safely kill processes using specific ports (Docker-aware)
safe_port_cleanup() {
    local port=$1
    echo "Cleaning up port $port..."
    
    # Get PIDs using the port, but exclude Docker processes
    local pids=$(sudo lsof -ti :$port 2>/dev/null | while read pid; do
        # Check if this is a Docker-related process
        local process_info=$(ps -p $pid -o comm= 2>/dev/null)
        if [[ ! "$process_info" =~ (docker|com.docker|Docker) ]]; then
            echo $pid
        fi
    done)
    
    if [ -n "$pids" ]; then
        for pid in $pids; do
            local process_name=$(ps -p $pid -o comm= 2>/dev/null)
            echo "Found non-Docker process on port $port: $process_name (PID: $pid)"
            safe_kill "$pid" "$process_name"
        done
    else
        echo "No non-Docker processes found using port $port"
    fi
}

# Kill existing tunnel processes from previous runs
if [ -f "$PID_FILE" ]; then
    echo "Cleaning up previous tunnel processes..."
    while read -r pid_line; do
        process_name=$(echo "$pid_line" | cut -d: -f1)
        pid=$(echo "$pid_line" | cut -d: -f2)
        safe_kill "$pid" "$process_name"
    done < "$PID_FILE"
    rm "$PID_FILE"
fi

# Clean up ports safely (avoid Docker processes)
safe_port_cleanup 5683
safe_port_cleanup 5684

# Kill only socat processes (not Docker processes)
echo "Cleaning up socat processes..."
pkill -f "socat.*5683" 2>/dev/null
pkill -f "socat.*5684" 2>/dev/null

# Verify Docker is still running
if ! docker info >/dev/null 2>&1; then
    echo "⚠️  Warning: Docker appears to be unavailable. You may need to restart Docker Desktop."
    echo "   This might have happened due to system processes being affected."
    echo "   Please restart Docker Desktop and try again."
    exit 1
fi

# Start tunnel components in background
echo "Starting SSH tunnel..."
ssh -R 5684:localhost:5684 pres -N &
SSH_PID=$!
echo "SSH_TUNNEL:$SSH_PID" > "$PID_FILE"

sleep 2

echo "Starting server-side UDP relay..."
ssh pres "sudo socat UDP4-LISTEN:5683,fork TCP:localhost:5684" &
SERVER_RELAY_PID=$!
echo "SERVER_RELAY:$SERVER_RELAY_PID" >> "$PID_FILE"

sleep 2

echo "Starting local TCP→UDP relay..."
socat TCP4-LISTEN:5684,fork UDP:localhost:5683 &
LOCAL_RELAY_PID=$!
echo "LOCAL_RELAY:$LOCAL_RELAY_PID" >> "$PID_FILE"

echo "UDP Tunnel started successfully!"
echo "SSH Tunnel PID: $SSH_PID"
echo "Server Relay PID: $SERVER_RELAY_PID"
echo "Local Relay PID: $LOCAL_RELAY_PID"
echo "PIDs saved to: $PID_FILE"

# Verify Docker is still working
if docker info >/dev/null 2>&1; then
    echo "✅ Docker daemon is still running"
else
    echo "⚠️  Warning: Docker daemon may have been affected"
fi

echo ""
echo "Monitor traffic with: sudo tcpdump -i any port 5683 -v"
echo "Start CoAP connector with: docker-compose up coap-connector"
echo "Stop tunnel with: ./udp_tunnel_stop.sh"