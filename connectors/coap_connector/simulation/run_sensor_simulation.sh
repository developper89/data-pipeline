#!/bin/bash

# Efento Sensor Simulation Runner
# Makes it easy to run sensor simulations with different configurations

set -e

# Default values
HOST="localhost"
PORT="5683"
SERIAL="KCwCQlJv"
# SERIAL="KCwCQk7t"

MEASUREMENT_INTERVAL="60"
TRANSMISSION_INTERVAL="60"  # Send every minute for testing
DURATION=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Efento Sensor Simulation Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --host HOST              CoAP server host (default: localhost)"
    echo "  -p, --port PORT              CoAP server port (default: 5683)"
    echo "  -s, --serial SERIAL          Device serial in base64 (default: KCwCQlJv)"
    echo "  -m, --measurement-interval   Measurement interval in seconds (default: 60)"
    echo "  -t, --transmission-interval  Transmission interval in seconds (default: 60)"
    echo "  -d, --duration MINUTES       Duration in minutes (default: infinite)"
    echo "  --help                       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Run with defaults"
    echo "  $0 --host 192.168.1.100              # Connect to remote server"
    echo "  $0 --transmission-interval 30         # Send every 30 seconds"
    echo "  $0 --duration 10                     # Run for 10 minutes"
    echo "  $0 --serial ABC123XYZ                 # Use different device serial"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -s|--serial)
            SERIAL="$2"
            shift 2
            ;;
        -m|--measurement-interval)
            MEASUREMENT_INTERVAL="$2"
            shift 2
            ;;
        -t|--transmission-interval)
            TRANSMISSION_INTERVAL="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Check if Python script exists
if [ ! -f "simulate_efento_sensor.py" ]; then
    print_error "simulate_efento_sensor.py not found in current directory"
    print_info "Make sure you're running this from the connectors/coap_connector directory"
    exit 1
fi

# Check if CoAP client is available
if ! command -v coap-client-notls &> /dev/null && ! command -v coap-client &> /dev/null; then
    print_warning "CoAP client not found. Installing libcoap-bin..."
    if command -v apt-get &> /dev/null; then
        sudo apt-get update && sudo apt-get install -y libcoap-bin
    elif command -v brew &> /dev/null; then
        brew install libcoap
    else
        print_error "Please install libcoap-bin or coap-utils manually"
        exit 1
    fi
fi

# Show configuration
print_info "Starting Efento Sensor Simulation"
echo "Configuration:"
echo "  Host: $HOST"
echo "  Port: $PORT"
echo "  Device Serial: $SERIAL"
echo "  Measurement Interval: ${MEASUREMENT_INTERVAL}s"
echo "  Transmission Interval: ${TRANSMISSION_INTERVAL}s"
if [ -n "$DURATION" ]; then
    echo "  Duration: ${DURATION} minutes"
else
    echo "  Duration: Infinite (Ctrl+C to stop)"
fi
echo ""

# Build Python command
PYTHON_CMD="python3 simulate_efento_sensor.py --host $HOST --port $PORT --serial $SERIAL --measurement-interval $MEASUREMENT_INTERVAL --transmission-interval $TRANSMISSION_INTERVAL"

if [ -n "$DURATION" ]; then
    PYTHON_CMD="$PYTHON_CMD --duration $DURATION"
fi

# Check if the target server is reachable
print_info "Testing connection to $HOST:$PORT..."
if timeout 5 bash -c "</dev/tcp/$HOST/$PORT" 2>/dev/null; then
    print_success "Server is reachable"
else
    print_warning "Cannot connect to $HOST:$PORT - server may not be running"
    print_info "Continuing anyway..."
fi

# Run the simulation
print_info "Starting simulation..."
echo "Press Ctrl+C to stop"
echo "=" * 50

# Execute the Python script
exec $PYTHON_CMD 