# Efento CoAP Connector Simulation Tools

This directory contains testing and simulation tools for the Efento CoAP connector.

## Files

### Core Testing Files

- **`test_efento_parser.py`** - Comprehensive test suite for the Efento bidirectional parser

  - Generates realistic test payloads based on actual sensor data
  - Tests parser functionality with normal and error conditions
  - Creates binary test files for CoAP client testing

- **`verify_protobuf_setup.py`** - Environment verification script
  - Checks protobuf installation and compiled modules
  - Verifies container environment setup
  - Useful for debugging protobuf import issues

### Sensor Simulation

- **`simulate_efento_sensor.py`** - Real-time sensor simulator

  - Acts like a real Efento sensor sending data every minute
  - Generates realistic temperature and humidity readings
  - Simulates sensor errors and battery status
  - Sends CoAP messages to your connector

- **`run_sensor_simulation.sh`** - Easy-to-use simulation runner
  - Shell script wrapper for the Python simulator
  - Configurable parameters (host, port, intervals, duration)
  - Automatic CoAP client detection and installation

### Documentation

- **`README_TESTING.md`** - Detailed testing documentation
  - Complete testing procedures and examples
  - Container environment setup instructions
  - CoAP client usage examples

## Quick Start

### 1. Run Basic Tests

```bash
cd simulation
python test_efento_parser.py
```

### 2. Verify Environment

```bash
python verify_protobuf_setup.py
```

### 3. Start Sensor Simulation

```bash
# Default: send data every minute to localhost:5683
./run_sensor_simulation.sh

# Custom configuration
./run_sensor_simulation.sh --host 192.168.1.100 --transmission-interval 30 --duration 10
```

### 4. Manual CoAP Testing

```bash
# Generate test files
python test_efento_parser.py

# Send with coap-client-notls
coap-client-notls -m post -f test_payload_normal.bin -T application/octet-stream coap://localhost:5683/m
```

## Simulation Features

The sensor simulator provides:

- **Realistic sensor behavior**: Temperature/humidity readings with natural drift
- **Error simulation**: Random sensor failures and error codes
- **Configurable timing**: Measurement and transmission intervals
- **Multiple endpoints**: Measurements (`/m`), device info (`/i`), config (`/c`)
- **Battery simulation**: Battery status changes over time
- **Continuous operation**: Runs indefinitely or for specified duration

## Integration Testing

1. **Start your CoAP connector** (in Docker or locally)
2. **Run the simulator** pointing to your connector
3. **Monitor logs** to see data flowing through your pipeline
4. **Verify data** reaches your final destination (InfluxDB, etc.)

## Troubleshooting

- **CoAP client not found**: Install with `sudo apt-get install libcoap-bin` or `brew install libcoap`
- **Protobuf import errors**: Run `verify_protobuf_setup.py` to check environment
- **Connection refused**: Ensure your CoAP connector is running and listening on the specified port
- **Parser errors**: Check that the bidirectional parser is properly installed in your connector

## Real Sensor Data

The test payloads are based on actual Efento sensor data:

- Device serial: "KCwCQlJv" (Base64) = "282c0242526f" (hex)
- Measurement timing: 60-second intervals
- Error detection: Handles sensor failure codes (8355840-8388607 range)
- Realistic measurements: Temperature 15-30°C, Humidity 30-80%
