#!/bin/bash
# udp_tunnel_stop2.sh

PID_FILE="./scripts/udp_tunnel_pids.txt"

echo "Stopping UDP Tunnel..."

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
    else
        echo "$process_name (PID: $pid) was already stopped"
    fi
}

if [ -f "$PID_FILE" ]; then
    echo "Stopping processes from PID file..."
    while read -r pid_line; do
        process_name=$(echo "$pid_line" | cut -d: -f1)
        pid=$(echo "$pid_line" | cut -d: -f2)
        safe_kill "$pid" "$process_name"
    done < "$PID_FILE"
    
    rm "$PID_FILE"
    echo "PID file removed"
else
    echo "No PID file found. Using targeted cleanup..."
    
    # Target specific socat processes only
    echo "Cleaning up socat processes..."
    pkill -f "socat.*5683" 2>/dev/null
    pkill -f "socat.*5684" 2>/dev/null
    
    # Target specific SSH tunnel (be very specific)
    echo "Cleaning up SSH tunnel processes..."
    pkill -f "ssh -R 5684:localhost:5684 pres" 2>/dev/null
fi

# Verify Docker is still working
if docker info >/dev/null 2>&1; then
    echo "✅ Docker daemon is still running"
    echo "UDP Tunnel stopped safely."
else
    echo "⚠️  Warning: Docker daemon appears to be unavailable"
    echo "   You may need to restart Docker Desktop"
    echo "   This shouldn't happen with the safe cleanup, but if it does:"
    echo "   1. Restart Docker Desktop"
    echo "   2. Wait for it to fully start"
    echo "   3. Try running your CoAP connector again"
fi