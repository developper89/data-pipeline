#!/bin/bash
# monitor_coap_server.sh - Monitor CoAP server availability

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

COAP_PORT=5683
CHECK_INTERVAL=2
LOG_FILE="./scripts/coap_server_monitor.log"

# Function to log with timestamp
log_status() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local status=$1
    local message=$2
    
    case $status in
        "UP")
            echo -e "${GREEN}[$timestamp] ✅ $message${NC}" | tee -a "$LOG_FILE"
            ;;
        "DOWN")
            echo -e "${RED}[$timestamp] ❌ $message${NC}" | tee -a "$LOG_FILE"
            ;;
        "WARN")
            echo -e "${YELLOW}[$timestamp] ⚠️  $message${NC}" | tee -a "$LOG_FILE"
            ;;
    esac
}

# Function to check CoAP server availability
check_coap_server() {
    # Method 1: Check if port is listening
    if nc -z -u localhost "$COAP_PORT" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to check Docker container
check_docker_container() {
    local container=$(docker ps --filter "publish=$COAP_PORT" --format "{{.Names}}" 2>/dev/null | head -1)
    if [ -n "$container" ]; then
        echo "$container"
        return 0
    else
        return 1
    fi
}

# Function to show current status
show_status() {
    echo -e "${GREEN}CoAP Server Monitor${NC}"
    echo "===================="
    echo "Port: $COAP_PORT"
    echo "Check interval: ${CHECK_INTERVAL}s"
    echo "Log file: $LOG_FILE"
    echo ""
    
    # Check current status
    if check_coap_server; then
        log_status "UP" "CoAP server is responding on port $COAP_PORT"
    else
        log_status "DOWN" "CoAP server is not responding on port $COAP_PORT"
    fi
    
    # Check Docker container
    local container=$(check_docker_container)
    if [ $? -eq 0 ]; then
        log_status "UP" "Docker container '$container' is running"
    else
        log_status "WARN" "No Docker container found publishing port $COAP_PORT"
    fi
}

# Function to monitor continuously
monitor_continuously() {
    local last_status=""
    local downtime_start=""
    
    echo "Starting continuous monitoring (Press Ctrl+C to stop)..."
    echo ""
    
    while true; do
        local current_status=""
        
        if check_coap_server; then
            current_status="UP"
            
            # Check if recovering from downtime
            if [ "$last_status" = "DOWN" ] && [ -n "$downtime_start" ]; then
                local downtime_duration=$(( $(date +%s) - $downtime_start ))
                log_status "UP" "CoAP server is back online (downtime: ${downtime_duration}s)"
                downtime_start=""
            elif [ "$last_status" != "UP" ]; then
                log_status "UP" "CoAP server is online"
            fi
        else
            current_status="DOWN"
            
            # Check if just went down
            if [ "$last_status" = "UP" ]; then
                downtime_start=$(date +%s)
                log_status "DOWN" "CoAP server went offline"
            elif [ "$last_status" != "DOWN" ]; then
                log_status "DOWN" "CoAP server is offline"
            fi
        fi
        
        last_status="$current_status"
        sleep "$CHECK_INTERVAL"
    done
}

# Handle Ctrl+C gracefully
trap 'echo -e "\n${GREEN}Monitoring stopped${NC}"; exit 0' SIGINT

# Main execution
case "${1:-}" in
    --status)
        show_status
        ;;
    --monitor)
        monitor_continuously
        ;;
    --test)
        echo "Testing CoAP server connectivity..."
        if check_coap_server; then
            echo "✅ CoAP server is responding"
            exit 0
        else
            echo "❌ CoAP server is not responding"
            exit 1
        fi
        ;;
    --help|-h)
        echo "Usage: $0 [option]"
        echo "Options:"
        echo "  --status   Show current CoAP server status"
        echo "  --monitor  Start continuous monitoring"
        echo "  --test     Test CoAP server availability (exit code 0=up, 1=down)"
        echo "  --help     Show this help message"
        echo "  (no args)  Show status and start monitoring"
        ;;
    *)
        show_status
        echo ""
        monitor_continuously
        ;;
esac 