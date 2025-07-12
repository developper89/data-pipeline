# Tunnel Status Checker

A comprehensive script to check the status of your UDP tunnel setup.

## Usage

```bash
# Full status check (recommended)
./scripts/check_tunnel_status.sh

# Check only local tunnel status
./scripts/check_tunnel_status.sh --local

# Check only remote tunnel status
./scripts/check_tunnel_status.sh --remote

# Run end-to-end connectivity test
./scripts/check_tunnel_status.sh --test

# Show tunnel architecture diagram
./scripts/check_tunnel_status.sh --arch

# Show monitoring commands
./scripts/check_tunnel_status.sh --monitor

# Show help
./scripts/check_tunnel_status.sh --help
```

## What It Checks

### Local Status

- ✅ PID file existence and process status
- ✅ SSH tunnel processes
- ✅ Socat relay processes
- ✅ Port usage (5683, 5684)
- ✅ Local connectivity
- ✅ Tunnel log file

### Remote Status

- ✅ SSH connection to remote server
- ✅ Remote socat processes
- ✅ Remote port usage
- ✅ Remote connectivity

### Output

- Color-coded status indicators
- Detailed process information
- Port usage details
- Tunnel architecture diagram
- Monitoring commands

## Quick Status Check

For a quick check, just run:

```bash
./scripts/check_tunnel_status.sh
```

This will show you everything you need to know about your tunnel status in one comprehensive report.
