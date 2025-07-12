#!/bin/bash
# check_tunnel_status.sh - Comprehensive tunnel status checker
# Usage: ./scripts/check_tunnel_status.sh

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PID_FILE="./scripts/udp_tunnel_pids.txt"
LOG_FILE="./scripts/tunnel.log"
REMOTE_HOST="pres"

# Logging function with colors
log_status() {
    local status=$1
    local message=$2
    case $status in
        "OK")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "FAIL")
            echo -e "${RED}❌ $message${NC}"
            ;;
        "WARN")
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
        "INFO")
            echo -e "${BLUE}ℹ️  $message${NC}"
            ;;
    esac
}

# Function to check if a process is running
check_process() {
    local pid=$1
    if kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to check local tunnel status
check_local_status() {
    echo -e "${BLUE}=== LOCAL TUNNEL STATUS ===${NC}"
    echo
    
    # Check PID file
    if [ -f "$PID_FILE" ]; then
        log_status "OK" "PID file exists"
        echo "Process IDs:"
        cat "$PID_FILE" | while read -r pid_line; do
            if [ -n "$pid_line" ]; then
                process_name=$(echo "$pid_line" | cut -d: -f1)
                pid=$(echo "$pid_line" | cut -d: -f2)
                
                case "$process_name" in
                    "SERVER_RELAY")
                        # Check remote process
                        if ssh -o ConnectTimeout=5 -o BatchMode=yes pres "kill -0 $pid" 2>/dev/null; then
                            log_status "OK" "$process_name (remote PID: $pid) is running"
                        else
                            log_status "FAIL" "$process_name (remote PID: $pid) is not running"
                        fi
                        ;;
                    *)
                        # Check local process
                        if check_process "$pid"; then
                            log_status "OK" "$process_name (PID: $pid) is running"
                        else
                            log_status "FAIL" "$process_name (PID: $pid) is not running"
                        fi
                        ;;
                esac
            fi
        done
    else
        log_status "WARN" "PID file not found at $PID_FILE"
    fi
    
    echo
    
    # Check for tunnel processes
    echo "Checking tunnel processes:"
    local ssh_processes=$(ps aux | grep -E "ssh.*$REMOTE_HOST" | grep -v grep | wc -l)
    local socat_processes=$(ps aux | grep -E "socat.*568" | grep -v grep | wc -l)
    
    if [ $ssh_processes -gt 0 ]; then
        log_status "OK" "SSH tunnel processes found ($ssh_processes)"
        ps aux | grep -E "ssh.*$REMOTE_HOST" | grep -v grep | while read -r line; do
            echo "  $line"
        done
    else
        log_status "FAIL" "No SSH tunnel processes found"
    fi
    
    if [ $socat_processes -gt 0 ]; then
        log_status "OK" "Socat processes found ($socat_processes)"
        ps aux | grep -E "socat.*568" | grep -v grep | while read -r line; do
            echo "  $line"
        done
    else
        log_status "FAIL" "No socat processes found"
    fi
    
    echo
    
    # Check ports
    echo "Checking local ports:"
    local port_5683=$(lsof -i :5683 2>/dev/null | grep -v COMMAND | wc -l)
    local port_5684=$(lsof -i :5684 2>/dev/null | grep -v COMMAND | wc -l)
    
    if [ $port_5683 -gt 0 ]; then
        log_status "OK" "Port 5683 is in use"
        lsof -i :5683 2>/dev/null | grep -v COMMAND | while read -r line; do
            echo "  $line"
        done
    else
        log_status "FAIL" "Port 5683 is not in use"
    fi
    
    if [ $port_5684 -gt 0 ]; then
        log_status "OK" "Port 5684 is in use"
        lsof -i :5684 2>/dev/null | grep -v COMMAND | while read -r line; do
            echo "  $line"
        done
    else
        log_status "FAIL" "Port 5684 is not in use"
    fi
    
    echo
    
    # Test local connectivity
    echo "Testing local connectivity:"
    if nc -z localhost 5684 2>/dev/null; then
        log_status "OK" "Port 5684 is responding"
    else
        log_status "FAIL" "Port 5684 is not responding"
    fi
    
    # Check tunnel log
    echo
    echo "Checking tunnel log:"
    if [ -f "$LOG_FILE" ]; then
        log_status "OK" "Log file exists"
        local log_size=$(wc -l < "$LOG_FILE")
        echo "Log file has $log_size lines. Latest entries:"
        tail -5 "$LOG_FILE" | while read -r line; do
            echo "  $line"
        done
    else
        log_status "WARN" "Log file not found at $LOG_FILE"
    fi
}

# Function to check remote tunnel status
check_remote_status() {
    echo -e "${BLUE}=== REMOTE TUNNEL STATUS ===${NC}"
    echo
    
    # Test SSH connection
    echo "Testing SSH connection:"
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "$REMOTE_HOST" exit 2>/dev/null; then
        log_status "OK" "SSH connection to $REMOTE_HOST is working"
    else
        log_status "FAIL" "SSH connection to $REMOTE_HOST failed"
        return 1
    fi
    
    echo
    
    # Check remote socat processes
    echo "Checking remote socat processes:"
    local remote_socat=$(ssh "$REMOTE_HOST" "ps aux | grep 'socat.*5683' | grep -v grep | wc -l" 2>/dev/null)
    
    if [ "$remote_socat" -gt 0 ]; then
        log_status "OK" "Remote socat processes found ($remote_socat)"
        ssh "$REMOTE_HOST" "ps aux | grep 'socat.*5683' | grep -v grep" 2>/dev/null | while read -r line; do
            echo "  $line"
        done
    else
        log_status "FAIL" "No remote socat processes found"
    fi
    
    echo
    
    # Check remote ports
    echo "Checking remote ports:"
    local remote_port_check=$(ssh "$REMOTE_HOST" "sudo lsof -i :5683 -i :5684 2>/dev/null | grep -v COMMAND | wc -l" 2>/dev/null)
    
    if [ "$remote_port_check" -gt 0 ]; then
        log_status "OK" "Remote ports are in use"
        ssh "$REMOTE_HOST" "sudo lsof -i :5683 -i :5684 2>/dev/null | grep -v COMMAND" 2>/dev/null | while read -r line; do
            echo "  $line"
        done
    else
        log_status "FAIL" "Remote ports are not in use"
    fi
    
    echo
    
    # Test remote connectivity (Note: nc -z doesn't work well with UDP, so this may show false negative)
    echo "Testing remote connectivity:"
    if ssh "$REMOTE_HOST" "nc -z localhost 5683" 2>/dev/null; then
        log_status "OK" "Remote port 5683 is responding"
    else
        log_status "WARN" "Remote port 5683 test failed (UDP ports are tricky to test)"
    fi
}

# Function to run end-to-end test
run_e2e_test() {
    echo -e "${BLUE}=== END-TO-END TEST ===${NC}"
    echo
    
    # Test 1: Basic UDP connectivity through tunnel
    echo "Testing tunnel UDP path:"
    if ssh "$REMOTE_HOST" "echo 'tunnel-test' | nc -u -w1 localhost 5683" 2>/dev/null; then
        log_status "OK" "Tunnel UDP path is working"
    else
        log_status "WARN" "Tunnel UDP path test inconclusive (normal for UDP)"
    fi
    
    # Test 2: Check if CoAP server is actually listening
    echo "Testing local CoAP server availability:"
    if nc -z -u localhost 5683 2>/dev/null; then
        log_status "OK" "Local CoAP server port is responding"
    else
        log_status "FAIL" "Local CoAP server port is not responding"
    fi
    
    # Test 3: Check if CoAP server is inside Docker
    echo "Checking Docker container status:"
    local coap_container=$(docker ps --filter "publish=5683" --format "{{.Names}}" 2>/dev/null | head -1)
    if [ -n "$coap_container" ]; then
        log_status "OK" "CoAP server container '$coap_container' is running"
        # Check container health if healthcheck is configured
        local health_status=$(docker inspect --format='{{.State.Health.Status}}' "$coap_container" 2>/dev/null)
        if [ "$health_status" = "healthy" ]; then
            log_status "OK" "CoAP server container is healthy"
        elif [ "$health_status" = "unhealthy" ]; then
            log_status "FAIL" "CoAP server container is unhealthy"
        elif [ -n "$health_status" ]; then
            log_status "WARN" "CoAP server container health: $health_status"
        fi
    else
        log_status "WARN" "No Docker container found publishing port 5683"
    fi
}

# Function to show tunnel architecture
show_architecture() {
    echo -e "${BLUE}=== TUNNEL ARCHITECTURE ===${NC}"
    echo
    echo "CoAP Device → Remote Server:5683 (UDP) → SSH Tunnel → Local:5684 (TCP) → Local:5683 (UDP) → Docker Container"
    echo
    echo "Flow:"
    echo "1. CoAP device sends UDP packets to $REMOTE_HOST:5683"
    echo "2. Remote socat forwards UDP:5683 → TCP:5684 (SSH tunnel)"
    echo "3. SSH tunnel forwards TCP:5684 → Local TCP:5684"
    echo "4. Local socat forwards TCP:5684 → UDP:5683"
    echo "5. Docker container receives UDP packets on localhost:5683"
}

# Function to show monitoring commands
show_monitoring_commands() {
    echo -e "${BLUE}=== MONITORING COMMANDS ===${NC}"
    echo
    echo "Continuous monitoring:"
    echo "  watch -n 5 'ps aux | grep -E \"(ssh.*$REMOTE_HOST|socat.*568)\" | grep -v grep'"
    echo
    echo "Port monitoring:"
    echo "  watch -n 5 'lsof -i :5683 -i :5684'"
    echo
    echo "Log monitoring:"
    echo "  tail -f $LOG_FILE"
    echo
    echo "Tunnel health test:"
    echo "  $0"
}

# Main execution
main() {
    echo -e "${GREEN}UDP Tunnel Status Checker${NC}"
    echo "=========================="
    echo
    
    check_local_status
    echo
    check_remote_status
    echo
    run_e2e_test
    echo
    show_architecture
    echo
    show_monitoring_commands
    
    echo
    echo -e "${GREEN}Status check completed!${NC}"
}

# Handle command line arguments
case "${1:-}" in
    --local)
        check_local_status
        ;;
    --remote)
        check_remote_status
        ;;
    --test)
        run_e2e_test
        ;;
    --arch)
        show_architecture
        ;;
    --monitor)
        show_monitoring_commands
        ;;
    --help|-h)
        echo "Usage: $0 [option]"
        echo "Options:"
        echo "  --local    Check local tunnel status only"
        echo "  --remote   Check remote tunnel status only"
        echo "  --test     Run end-to-end test only"
        echo "  --arch     Show tunnel architecture"
        echo "  --monitor  Show monitoring commands"
        echo "  --help     Show this help message"
        echo "  (no args) Run full status check"
        ;;
    *)
        main
        ;;
esac 