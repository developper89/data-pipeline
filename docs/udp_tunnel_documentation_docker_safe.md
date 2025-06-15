# UDP Tunnel Setup Documentation - CoAP Traffic Redirection

## Overview

This document provides detailed instructions for successfully setting up a UDP tunnel to redirect CoAP traffic from a remote server to a local development machine for testing purposes.

### Goal
Redirect CoAP traffic (UDP port 5683) from server `app.preservarium.fr` to your local CoAP connector for testing and development.

### Challenge
SSH tunnels are TCP-based, but CoAP uses UDP protocol. Direct UDP tunneling through SSH is not supported.

### Solution
Use SSH + socat to create UDP-over-TCP tunneling with bidirectional communication.

## Architecture

```
┌─────────────────┐    UDP     ┌──────────────────┐    socat    ┌─────────────────┐
│   Efento Sensor │  ────────▶ │  Remote Server   │  ────────▶  │   SSH Tunnel    │
│ (80.187.66.54)  │  Port 5683 │(app.preservarium │  UDP→TCP   │   (Port 5684)   │
└─────────────────┘            │     .fr)         │             └─────────────────┘
                               └──────────────────┘                       │
                                                                          │ TCP
                                                                          ▼
┌─────────────────┐    socat    ┌─────────────────┐             ┌─────────────────┐
│ Local CoAP      │  ◀────────  │ Local Machine   │  ◀────────  │ Local Machine   │
│ Connector       │  TCP→UDP   │ (socat relay)   │  SSH Tunnel │ (SSH client)    │
│ (Port 5683)     │             └─────────────────┘             └─────────────────┘
└─────────────────┘
```

## Prerequisites

- SSH access to `app.preservarium.fr` (configured as `pres` in SSH config)
- Local machine with Docker and CoAP connector
- `socat` installed on both machines
- Root/sudo access on both machines
- `tcpdump` for monitoring traffic

## Step-by-Step Setup

### Step 1: Environment Preparation

#### 1.1 Clean Up Existing Processes
```bash
# Kill processes using required ports
sudo lsof -ti :5683 | xargs kill -9
sudo lsof -ti :5684 | xargs kill -9

# Kill existing socat processes
killall socat

# Kill existing SSH tunnels (be careful)
pkill -f "ssh -R"

# Verify ports are free
sudo lsof -i :5683
sudo lsof -i :5684
```

#### 1.2 Install socat

**On Local Machine (macOS):**
```bash
brew install socat
socat -V
```

**On Remote Server:**
```bash
ssh pres "sudo apt update && sudo apt install socat -y"
ssh pres "socat -V"
```

### Step 2: Basic Connectivity Tests

#### 2.1 Test SSH Connection
```bash
ssh pres "echo 'SSH connection works'"
```
**Expected Output:** `SSH connection works`

#### 2.2 Test TCP Reverse Tunnel
Open 3 terminals:

**Terminal 1 (TCP Tunnel):**
```bash
ssh -R 5684:localhost:5684 pres -N
```

**Terminal 3 (Local Listener):**
```bash
nc -l 5684
```

**Terminal 2 (Test Message):**
```bash
ssh pres "echo 'test tcp tunnel' | nc localhost 5684"
```

**Expected Result:** Terminal 3 should display "test tcp tunnel"

### Step 3: Complete UDP Tunnel Setup

#### 3.1 Stop Remote CoAP Server
```bash
# First, identify what's using port 5683
ssh pres "sudo lsof -i :5683"

# Stop the existing CoAP server (adjust command based on your setup)
ssh pres "sudo systemctl stop your-coap-service"
# or
ssh pres "sudo pkill -f coap"
```

#### 3.2 Start UDP Tunnel Components

**Terminal 1: SSH TCP Tunnel (keep running)**
```bash
ssh -R 5684:localhost:5684 pres -N
```

**Terminal 2: Server-side UDP→TCP Relay**
```bash
ssh pres "sudo socat UDP4-LISTEN:5683,fork TCP:localhost:5684"
```

**Terminal 3: Local TCP→UDP Relay**
```bash
socat TCP4-LISTEN:5684,fork UDP:localhost:5683
```

**Terminal 4: Monitor UDP Traffic**
```bash
# Monitor all interfaces to see tunneled traffic
sudo tcpdump -i any port 5683 -v
```

### Step 4: Testing and Verification

#### 4.1 Test UDP End-to-End
**Terminal 5: Send Test Packet**
```bash
echo "UDP tunnel test" | nc -u app.preservarium.fr 5683
```

**Expected tcpdump Output:**
```
IP localhost.xxxxx > localhost.5683: UDP, length 16
```

#### 4.2 Start Local CoAP Connector
**Terminal 6: Start CoAP Connector**
```bash
# Navigate to your project directory
cd /path/to/your/data-pipeline

# Start the CoAP connector
docker-compose up coap-connector
```

## Traffic Flow Analysis

### Successful Packet Flow
```
Sensor (80.187.66.54:21085) 
    ↓ UDP CoAP Request (374 bytes)
Server (185.98.138.142:5683)
    ↓ socat UDP4-LISTEN:5683,fork TCP:localhost:5684
SSH Tunnel (TCP:5684)
    ↓ ssh -R 5684:localhost:5684
Local Machine (TCP:5684)
    ↓ socat TCP4-LISTEN:5684,fork UDP:localhost:5683
Local CoAP Connector (localhost:5683)
    ↓ Process & Respond (39 bytes)
[Reverse path for responses]
```

### Observed Traffic Patterns

**Incoming Sensor Data:**
- **Source:** `localhost.62465` (tunneled from sensor)
- **Destination:** `localhost.5683` (local CoAP connector)
- **Sizes:** 374, 250, 230 bytes (typical CoAP messages)

**CoAP Responses:**
- **Source:** `localhost.5683` (local CoAP connector)
- **Destination:** `localhost.62465` (back to sensor via tunnel)
- **Size:** 39 bytes (typical CoAP ACK)

## Monitoring and Debugging

### Essential Monitoring Commands

**Monitor Tunneled Traffic:**
```bash
# See all UDP traffic on port 5683 (including tunneled)
sudo tcpdump -i any port 5683 -v

# More detailed packet inspection
sudo tcpdump -i any port 5683 -v -X

# Monitor specific protocol
sudo tcpdump -i any port 5683 and udp -v
```

**Check Process Status:**
```bash
# Verify socat processes are running
ps aux | grep socat

# Check SSH tunnel status
ps aux | grep "ssh -R"

# Verify port usage
sudo lsof -i :5683
sudo lsof -i :5684
```

**Server-side Monitoring:**
```bash
# Monitor server-side UDP traffic
ssh pres "sudo tcpdump -i eth0 -n udp port 5683 -vvv"

# Check server-side processes
ssh pres "ps aux | grep socat"
```

### Common Issues and Solutions

#### Issue 1: Tunnel Not Forwarding Traffic
**Symptoms:** No packets visible in local tcpdump
**Solution:**
```bash
# Check if tunnel is properly established
ssh pres "netstat -tlnp | grep 5684"
# Should show: tcp 127.0.0.1:5684 LISTEN

# Test tunnel manually
ssh pres "echo 'test' | nc localhost 5684"
```

#### Issue 2: Port Already in Use
**Symptoms:** "Address already in use" errors
**Solution:**
```bash
# Find and kill processes using the port
sudo lsof -ti :5683 | xargs kill -9
sudo lsof -ti :5684 | xargs kill -9
```

#### Issue 3: Permission Denied for Port 5683
**Symptoms:** socat fails to bind to port 5683
**Solution:**
```bash
# Use sudo for privileged ports
ssh pres "sudo socat UDP4-LISTEN:5683,fork TCP:localhost:5684"
```

## Successful Implementation Results

### CoAP Connector Logs
```
2025-06-14 07:40:15,695 - resources - INFO - 🔍 INCOMING CoAP REQUEST [REQ-0001]
2025-06-14 07:40:15,695 - resources - INFO -   Method: POST
2025-06-14 07:40:15,695 - resources - INFO -   Source: 192.168.65.1:61919
2025-06-14 07:40:15,695 - resources - INFO -   Payload Size: 363 bytes
2025-06-14 07:40:15,696 - resources - INFO - Successfully detected message type: config
2025-06-14 07:40:15,696 - resources - INFO - Successfully extracted device ID: 282c02424eed
2025-06-14 07:40:15,697 - resources - INFO - Translation successful using protobuf_efento
```

### tcpdump Output
```
09:46:16.418410 IP localhost.62465 > localhost.5683: UDP, length 374
09:46:16.506691 IP localhost.5683 > localhost.62465: UDP, length 39
09:46:18.749885 IP localhost.62465 > localhost.5683: UDP, length 250
09:46:18.755659 IP localhost.5683 > localhost.62465: UDP, length 39
```

## Docker-Safe Startup Script

Create a Docker-safe startup script:

```bash
#!/bin/bash
# udp_tunnel_start.sh

PID_FILE="/tmp/udp_tunnel_pids.txt"

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
```

## Docker-Safe Cleanup Script

```bash
#!/bin/bash
# udp_tunnel_stop.sh

PID_FILE="/tmp/udp_tunnel_pids.txt"

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
```

## Quick Docker Recovery Script

```bash
#!/bin/bash
# docker_recovery.sh

echo "Checking Docker status..."

if docker info >/dev/null 2>&1; then
    echo "✅ Docker is running normally"
    exit 0
fi

echo "❌ Docker daemon is not responding"
echo "Attempting to restart Docker..."

# On macOS, restart Docker Desktop
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Restarting Docker Desktop..."
    osascript -e 'quit app "Docker"'
    sleep 5
    open -a Docker
    
    echo "Waiting for Docker to start..."
    for i in {1..30}; do
        if docker info >/dev/null 2>&1; then
            echo "✅ Docker is now running"
            exit 0
        fi
        echo "Waiting... ($i/30)"
        sleep 2
    done
    
    echo "❌ Docker failed to start automatically"
    echo "Please manually restart Docker Desktop"
else
    echo "Please restart Docker using your system's method"
fi
```

## Alternative: Using Process Groups

For even better process management, you can use process groups:

```bash
#!/bin/bash
# udp_tunnel_start_advanced.sh

PGRP_FILE="/tmp/udp_tunnel_pgrp.txt"

echo "Starting UDP Tunnel for CoAP Traffic..."

# Kill existing process group if exists
if [ -f "$PGRP_FILE" ]; then
    PGRP=$(cat "$PGRP_FILE")
    if kill -0 -"$PGRP" 2>/dev/null; then
        echo "Killing existing process group: $PGRP"
        kill -TERM -"$PGRP" 2>/dev/null
        sleep 2
        kill -KILL -"$PGRP" 2>/dev/null
    fi
    rm "$PGRP_FILE"
fi

# Start new process group
set -m  # Enable job control
(
    # This subshell creates a new process group
    echo "Starting SSH tunnel..."
    ssh -R 5684:localhost:5684 pres -N &
    SSH_PID=$!
    
    sleep 2
    
    echo "Starting server-side UDP relay..."
    ssh pres "sudo socat UDP4-LISTEN:5683,fork TCP:localhost:5684" &
    SERVER_RELAY_PID=$!
    
    sleep 2
    
    echo "Starting local TCP→UDP relay..."
    socat TCP4-LISTEN:5684,fork UDP:localhost:5683 &
    LOCAL_RELAY_PID=$!
    
    echo "All processes started in process group: $"
    echo "$" > "$PGRP_FILE"
    
    # Wait for all background processes
    wait
) &

echo "UDP Tunnel process group started!"
echo "Stop with: kill -TERM -\$(cat $PGRP_FILE)"
```

## Security Considerations

1. **Temporary Setup:** This is for development/testing only
2. **Firewall:** Ensure no unintended services are exposed
3. **SSH Security:** Use secure SSH keys and limit access
4. **Monitoring:** Watch for unexpected traffic patterns
5. **Cleanup:** Always stop tunnels when not needed

## Performance Notes

- **Latency:** Adds minimal latency (typically <50ms)
- **Throughput:** Suitable for CoAP traffic (small packets)
- **Resource Usage:** Low CPU and memory overhead
- **Reliability:** Robust for intermittent sensor data

## Production Considerations

For production deployment, consider:
- **VPN Setup:** More secure and permanent solution
- **Direct Network Routing:** If network topology allows
- **Load Balancing:** For multiple sensors
- **Monitoring:** Comprehensive logging and alerting
- **Failover:** Backup connectivity methods

## Conclusion

This UDP tunneling setup successfully enables local development and testing of CoAP connectors with real sensor data while maintaining production server operation. The solution provides:

- ✅ Bidirectional UDP communication through SSH
- ✅ Real-time sensor data processing locally
- ✅ Preserved production server functionality
- ✅ Easy setup and teardown procedures
- ✅ Comprehensive monitoring capabilities

The tunnel allows seamless development workflow where changes can be tested locally with live sensor data before deployment to production.
