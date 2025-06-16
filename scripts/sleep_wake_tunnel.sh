#!/bin/bash
# sleep_wake_tunnel.sh - Handle tunnel on macOS sleep/wake events

TUNNEL_SCRIPT="./scripts/udp_tunnel_start.sh"
LOG_FILE="./scripts/sleep_wake.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Monitor for sleep/wake events
pmset -g log | grep -E "(Sleep|Wake)" --line-buffered | while read line; do
    if echo "$line" | grep -q "Sleep"; then
        log "System going to sleep - stopping tunnel"
        ./scripts/udp_tunnel_stop.sh
    elif echo "$line" | grep -q "Wake"; then
        log "System woke up - restarting tunnel in 10 seconds"
        sleep 10  # Give system time to fully wake up
        $TUNNEL_SCRIPT
    fi
done