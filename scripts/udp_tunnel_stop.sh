# ==============================================================================
# 2. udp_tunnel_stop.sh - Tunnel stop script
# ==============================================================================
#!/bin/bash
# udp_tunnel_stop.sh

PID_FILE="./scripts/udp_tunnel_pids.txt"

echo "Stopping UDP Tunnel..."

# Function to safely kill a process by PID
safe_kill() {
    local pid=$1
    local process_name=$2
    
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null
        sleep 2
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
    
    local pids=$(sudo lsof -ti :$port 2>/dev/null | while read pid; do
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
    fi
}

if [ -f "$PID_FILE" ]; then
    echo "Killing tunnel processes from PID file..."
    while read -r pid_line; do
        if [ -n "$pid_line" ]; then
            process_name=$(echo "$pid_line" | cut -d: -f1)
            pid=$(echo "$pid_line" | cut -d: -f2)
            safe_kill "$pid" "$process_name"
        fi
    done < "$PID_FILE"
    rm "$PID_FILE"
else
    echo "No PID file found. Using targeted cleanup..."
fi

# Clean up any remaining processes
echo "Cleaning up socat processes..."
pkill -f "socat.*5683" 2>/dev/null
pkill -f "socat.*5684" 2>/dev/null

echo "Cleaning up SSH tunnel processes..."
pkill -f "ssh.*5684.*pres" 2>/dev/null

# Clean up ports
safe_port_cleanup 5683
safe_port_cleanup 5684

# Clean up remote socat processes
echo "Cleaning up remote socat processes..."
ssh pres "sudo pkill -f 'socat.*5683'" 2>/dev/null || true

# Verify Docker is still working
if docker info >/dev/null 2>&1; then
    echo "✅ Docker daemon is still running"
else
    echo "⚠️  Warning: Docker daemon may have been affected"
fi

echo "UDP Tunnel stopped safely."