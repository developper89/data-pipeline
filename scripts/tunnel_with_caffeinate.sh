# ==============================================================================
# 5. tunnel_with_caffeinate.sh - Prevent sleep and maintain tunnel
# ==============================================================================
#!/bin/bash
# tunnel_with_caffeinate.sh - Prevent sleep and maintain tunnel

TUNNEL_SCRIPT="./scripts/udp_tunnel_start.sh"
TUNNEL_STOP_SCRIPT="./scripts/udp_tunnel_stop.sh"
LOG_FILE="./scripts/tunnel_caffeinate.log"
PID_FILE="./scripts/caffeinate.pid"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to cleanup on exit
cleanup() {
    log "Cleaning up caffeinate and tunnel..."
    if [ -f "$PID_FILE" ]; then
        CAFFEINATE_PID=$(cat "$PID_FILE")
        kill $CAFFEINATE_PID 2>/dev/null
        rm "$PID_FILE"
        log "Stopped caffeinate (PID: $CAFFEINATE_PID)"
    fi
    $TUNNEL_STOP_SCRIPT
    log "Tunnel maintenance stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

log "Starting tunnel with sleep prevention..."

# Start tunnel
log "Starting initial tunnel..."
$TUNNEL_SCRIPT

# Prevent system sleep while maintaining tunnel
log "Starting caffeinate to prevent system sleep..."
caffeinate -i -s &
CAFFEINATE_PID=$!
echo $CAFFEINATE_PID > "$PID_FILE"
log "Caffeinate started (PID: $CAFFEINATE_PID)"

# Keep script running and monitor tunnel health
log "Monitoring tunnel health every 5 minutes..."
while true; do
    sleep 300  # Check every 5 minutes
    
    # Simple tunnel health check
    if ! pgrep -f "ssh.*5684" > /dev/null; then
        log "SSH tunnel not found, restarting..."
        $TUNNEL_SCRIPT
    fi
    
    # Check if caffeinate is still running
    if ! kill -0 $CAFFEINATE_PID 2>/dev/null; then
        log "Caffeinate died, restarting..."
        caffeinate -i -s &
        CAFFEINATE_PID=$!
        echo $CAFFEINATE_PID > "$PID_FILE"
        log "Caffeinate restarted (PID: $CAFFEINATE_PID)"
    fi
done