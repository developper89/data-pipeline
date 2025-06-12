# Preservarium Pusher API

A FastAPI-based web service for pushing commands to IoT devices through Kafka. This service provides a RESTful API for sending device commands that are published to Kafka topics for consumption by device connectors.

## Features

- RESTful API for device command publishing
- Protocol-agnostic design supporting MQTT and CoAP
- Kafka integration for reliable message delivery
- Optional API key authentication
- Comprehensive health checks and metrics
- Batch command processing
- Docker support with multi-stage builds
- Structured logging with JSON/text formats

## Architecture

The service follows a clean architecture pattern:

```
preservarium-pusher-api/
├── pusher_api/
│   ├── api/                    # API layer (FastAPI routes)
│   │   ├── dependencies.py     # Dependency injection
│   │   └── v1/
│   │       ├── commands.py     # Command endpoints
│   │       └── health.py       # Health endpoints
│   ├── core/                   # Core configuration and models
│   │   ├── config.py          # Settings and configuration
│   │   ├── exceptions.py      # Custom exceptions
│   │   └── models.py          # API-specific Pydantic models
│   ├── services/              # Business logic layer
│   │   ├── pusher_service.py  # Main service logic
│   │   └── kafka_service.py   # Kafka integration
│   └── main.py                # FastAPI application factory
└── shared/                    # Shared models and utilities
    └── models/
        └── common.py          # CommandMessage and other shared models
```

## Command Model

Commands use the shared `CommandMessage` model from `shared.models.common`:

```python
class CommandMessage(CustomBaseModel):
    device_id: str              # Target device identifier
    command_type: str           # Command type (e.g., 'reboot', 'config_update')
    payload: Dict[str, Any]     # Command payload data
    protocol: str               # Protocol ('mqtt' or 'coap')
    parser_script_ref: Optional[str]  # Parser script reference
    metadata: Optional[Dict]    # Protocol-specific metadata
    priority: Optional[int]     # Command priority
    expires_at: Optional[datetime]  # Expiration time

    # Inherited from CustomBaseModel:
    request_id: str             # Auto-generated UUID
    timestamp: datetime         # Auto-generated timestamp
```

## Installation

### Using Docker (Recommended)

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd data-pipeline/preservarium-pusher-api
   ```

2. **Build and run with Docker Compose:**
   ```bash
   docker-compose up --build
   ```

This will start:

- Preservarium Pusher API on port 8000
- Kafka on port 9092
- Zookeeper on port 2181
- Kafka UI on port 8080

### Local Development

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables:**

   ```bash
   export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
   export KAFKA_COMMAND_TOPIC="device_commands"
   # Optional: Enable API key authentication
   export API_KEYS="your-secret-api-key-1,your-secret-api-key-2"
   ```

3. **Run the service:**
   ```bash
   uvicorn pusher_api.main:create_app --factory --host 0.0.0.0 --port 8000 --reload
   ```

## Configuration

The service is configured through environment variables:

| Variable                  | Default                   | Description                         |
| ------------------------- | ------------------------- | ----------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`          | Kafka bootstrap servers             |
| `KAFKA_COMMAND_TOPIC`     | `device_commands`         | Kafka topic for commands            |
| `KAFKA_TIMEOUT_MS`        | `5000`                    | Kafka operation timeout             |
| `API_HOST`                | `0.0.0.0`                 | API host binding                    |
| `API_PORT`                | `8000`                    | API port                            |
| `API_TITLE`               | `Preservarium Pusher API` | API title                           |
| `API_VERSION`             | `1.0.0`                   | API version                         |
| `CORS_ORIGINS`            | `["*"]`                   | CORS allowed origins                |
| `LOG_LEVEL`               | `INFO`                    | Logging level                       |
| `LOG_FORMAT`              | `text`                    | Log format (`text` or `json`)       |
| `API_KEYS`                | `None`                    | Comma-separated API keys (optional) |
| `ENVIRONMENT`             | `development`             | Environment name                    |
| `SERVICE_NAME`            | `preservarium-pusher-api` | Service name                        |

## API Documentation

Once the service is running, you can access:

- **Interactive API docs (Swagger UI):** http://localhost:8000/docs
- **Alternative API docs (ReDoc):** http://localhost:8000/redoc
- **OpenAPI schema:** http://localhost:8000/openapi.json

## Usage Examples

### Basic Health Check

```bash
curl http://localhost:8000/health
```

### Send a Single Command

```bash
curl -X POST "http://localhost:8000/api/v1/commands" \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "efento_device_001",
    "command_type": "config_update",
    "payload": {"measurement_interval": 600},
    "protocol": "coap",
    "parser_script_ref": "efento_bidirectional_parser.py",
    "metadata": {
      "confirmable": true,
      "options": {"observe": 0}
    }
  }'
```

### Send Batch Commands

```bash
curl -X POST "http://localhost:8000/api/v1/commands/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "commands": [
      {
        "device_id": "device_001",
        "command_type": "reboot",
        "payload": {},
        "protocol": "coap",
        "metadata": {"confirmable": true}
      },
      {
        "device_id": "device_002",
        "command_type": "update",
        "payload": {"version": "1.2.3"},
        "protocol": "mqtt",
        "metadata": {
          "topic": "devices/device_002/commands",
          "qos": 1,
          "retain": false
        }
      }
    ]
  }'
```

### With API Key Authentication

```bash
curl -X POST "http://localhost:8000/api/v1/commands" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "device_id": "secure_device_001",
    "command_type": "security_update",
    "payload": {"enable_encryption": true},
    "protocol": "mqtt"
  }'
```

### Get Service Status

```bash
curl http://localhost:8000/api/v1/status
```

## Protocol-Specific Examples

### MQTT Commands

```json
{
  "device_id": "mqtt_device_001",
  "command_type": "led_control",
  "payload": { "state": "on", "brightness": 80 },
  "protocol": "mqtt",
  "metadata": {
    "topic": "devices/mqtt_device_001/commands/led",
    "qos": 1,
    "retain": false
  }
}
```

### CoAP Commands

```json
{
  "device_id": "coap_device_001",
  "command_type": "sensor_config",
  "payload": { "sampling_rate": 10 },
  "protocol": "coap",
  "metadata": {
    "confirmable": true,
    "options": { "uri_path": "/config" }
  }
}
```

## Client Libraries

### Python Client

```python
import requests
import json

class PusherAPIClient:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key

    def _headers(self):
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def push_command(self, device_id: str, command_type: str,
                    payload: dict, protocol: str, **kwargs):
        command = {
            "device_id": device_id,
            "command_type": command_type,
            "payload": payload,
            "protocol": protocol,
            **kwargs
        }

        response = requests.post(
            f"{self.base_url}/api/v1/commands",
            headers=self._headers(),
            data=json.dumps(command)
        )
        response.raise_for_status()
        return response.json()

    def health_check(self):
        response = requests.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

# Usage
client = PusherAPIClient("http://localhost:8000", api_key="your-key")

result = client.push_command(
    device_id="test_device",
    command_type="reboot",
    payload={},
    protocol="mqtt",
    metadata={"topic": "devices/test_device/commands", "qos": 1}
)
print(result)
```

### JavaScript Client

```javascript
class PusherAPIClient {
  constructor(baseURL, apiKey = null) {
    this.baseURL = baseURL.replace(/\/$/, "");
    this.apiKey = apiKey;
  }

  _headers() {
    const headers = { "Content-Type": "application/json" };
    if (this.apiKey) {
      headers["X-API-Key"] = this.apiKey;
    }
    return headers;
  }

  async pushCommand(deviceId, commandType, payload, protocol, options = {}) {
    const command = {
      device_id: deviceId,
      command_type: commandType,
      payload: payload,
      protocol: protocol,
      ...options,
    };

    const response = await fetch(`${this.baseURL}/api/v1/commands`, {
      method: "POST",
      headers: this._headers(),
      body: JSON.stringify(command),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  }

  async healthCheck() {
    const response = await fetch(`${this.baseURL}/health`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  }
}

// Usage
const client = new PusherAPIClient("http://localhost:8000", "your-key");

client
  .pushCommand(
    "test_device",
    "led_control",
    { state: "on", color: "blue" },
    "mqtt",
    { metadata: { topic: "devices/test_device/led", qos: 1 } }
  )
  .then((result) => console.log(result));
```

## Development

### Running Tests

```bash
# Install test dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=pusher_api --cov-report=html
```

### Code Quality

```bash
# Format code
black pusher_api/
isort pusher_api/

# Lint code
flake8 pusher_api/
mypy pusher_api/
```

## Monitoring and Debugging

### Health Endpoints

- **Basic Health:** `GET /health` - Simple health check
- **Detailed Health:** `GET /api/v1/health` - Detailed service status
- **Service Status:** `GET /api/v1/status` - Comprehensive metrics

### Logging

The service provides structured logging with configurable formats:

```bash
# JSON logging (for production)
export LOG_FORMAT=json

# Text logging (for development)
export LOG_FORMAT=text
```

### Metrics

Service metrics are available at `/api/v1/status`:

```json
{
  "service": "preservarium-pusher-api",
  "version": "1.0.0",
  "environment": "production",
  "kafka_connected": true,
  "uptime": "2 hours, 30 minutes",
  "metrics": {
    "commands_sent_total": 1250,
    "commands_failed_total": 5,
    "uptime_seconds": 9000.0,
    "kafka_connected": true,
    "last_error": null,
    "last_error_time": null
  }
}
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**

   - Check `KAFKA_BOOTSTRAP_SERVERS` configuration
   - Ensure Kafka is running and accessible
   - Verify network connectivity

2. **Command Validation Errors**

   - Check CommandMessage schema requirements
   - Ensure all required fields are provided
   - Validate protocol-specific metadata format

3. **Authentication Issues**
   - Verify API key configuration
   - Check `X-API-Key` header format
   - Ensure API keys are properly comma-separated in environment

### Debug Mode

Enable debug logging for troubleshooting:

```bash
export LOG_LEVEL=DEBUG
uvicorn pusher_api.main:create_app --factory --reload
```

## License

This project is part of the Preservarium IoT data pipeline system.

# ProtoMeasurements Parser

A comprehensive Python parser for IoT sensor measurement data from CoAP payloads using the ProtoMeasurements protobuf schema.

## Overview

This parser extracts complete IoT sensor data from binary protobuf payloads, including:

- Device identification and battery status
- Measurement periods and timestamps
- Multiple sensor channels (Temperature, Humidity, Light, etc.)
- Error handling and sensor failure detection
- Human-readable output with proper unit conversions

## Files

- `protobuf_parser.py` - Main parser implementation
- `parse_custom_payload.py` - Example usage with custom payloads
- `docs/protobuf-measurements-parsing.md` - Comprehensive documentation
- `docs/proto_measurements.proto` - Protobuf schema definition

## Quick Start

### Basic Usage

```python
from protobuf_parser import ProtoMeasurementsParser

# Your payload as hex string
payload_hex = "0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000..."
payload_bytes = bytes.fromhex(payload_hex)

# Parse the payload
parser = ProtoMeasurementsParser()
result = parser.parse_complete_message(payload_bytes)

# Print human-readable results
parser.print_parsed_data(result)
```

### Command Line Usage

Run the parser with the example payload:

```bash
python protobuf_parser.py
```

Parse your own payload:

```bash
python parse_custom_payload.py
```

## Features

### Supported Fields

- **Device Serial Number** - Unique device identifier
- **Battery Status** - Battery health (OK/LOW)
- **Measurement Periods** - Base period and factor calculations
- **Sensor Channels** - Multiple sensor types with measurements
- **Timestamps** - Unix timestamps with ISO 8601 conversion
- **Transfer Reasons** - Bit-flag decoded transmission reasons
- **Configuration Hash** - Device configuration tracking
- **Cloud Token** - Optional cloud service integration

### Supported Sensor Types

| Type | Name          | Unit  | Precision |
| ---- | ------------- | ----- | --------- |
| 1    | Temperature   | °C    | 0.1°C     |
| 2    | Humidity      | %RH   | 0.1%      |
| 3    | Light         | lux   | 1 lux     |
| 4    | Accelerometer | m/s²  | -         |
| 5    | Digital Input | state | Binary    |
| 6    | Pressure      | hPa   | 0.1 hPa   |
| 7    | CO2           | ppm   | 1 ppm     |
| 8    | Voltage       | mV    | 1 mV      |
| 9    | Current       | mA    | 1 mA      |
| 10   | Power         | mW    | 1 mW      |

### Error Handling

The parser automatically detects and handles:

- Sensor error codes (8355840-8388607 range)
- Malformed payloads
- Missing or invalid fields
- Timestamp validation
- Unknown sensor types

## Sample Output

```
============================================================
PROTOBUF MEASUREMENTS PARSER RESULTS
============================================================
📱 Device ID: 282c02424eed
🔋 Battery Status: OK
⏱️  Measurement Period: 60s (base) × 1 (factor) = 60s
📊 Active Channels: 2
🔧 Config Hash: 9
📡 Next Transmission: 2025-06-11T20:44:06+00:00
🕒 Parsed at: 2025-06-12T11:55:38+00:00
📏 Payload size: 58 bytes

============================================================
SENSOR CHANNELS
============================================================

📡 Channel 1: Temperature (Type 1)
   🕐 Base Timestamp: 2025-06-11T20:34:00+00:00
   🎯 Start Point: 233
   📈 Sample Count: 2
   📊 Measurements:
      1. 23.3°C at 2025-06-11T20:34:00+00:00
      2. 23.3°C at 2025-06-11T20:35:00+00:00

📡 Channel 2: Humidity (Type 2)
   🕐 Base Timestamp: 2025-06-11T20:34:00+00:00
   🎯 Start Point: 57
   📈 Sample Count: 2
   📊 Measurements:
      1. 5.7%RH at 2025-06-11T20:34:00+00:00
      2. 5.7%RH at 2025-06-11T20:35:00+00:00
```

## Programmatic Access

The parser returns a comprehensive dictionary with all parsed data:

```python
result = parser.parse_complete_message(payload_bytes)

# Access parsed data
device_id = result['device_id']
battery_ok = result['battery_ok']
channels = result['channels']

# Access measurements
for channel in channels:
    sensor_type = channel['type_name']
    measurements = channel['measurements']

    for measurement in measurements:
        if measurement['status'] == 'ok':
            value = measurement['value']
            unit = measurement['unit']
            timestamp = measurement['timestamp_iso']
            print(f"{sensor_type}: {value}{unit} at {timestamp}")
```

## JSON Export

The parser can export results to JSON:

```python
import json

result = parser.parse_complete_message(payload_bytes)

# Save to JSON file
with open('parsed_result.json', 'w') as f:
    json.dump(result, f, indent=2, default=str)
```

## Advanced Usage

### Binary Sensor Processing

For binary sensors (like Digital Input), the parser handles state changes:

```python
# Binary sensor offsets represent state changes over time
# Positive values = HIGH state, Negative values = LOW state
# Absolute value = time offset from base timestamp
```

### Error Code Handling

The parser automatically detects sensor errors:

```python
for measurement in measurements:
    if measurement['status'] == 'sensor_error':
        error_code = measurement['error_code']
        description = measurement['error_description']
        print(f"Sensor Error {error_code}: {description}")
```

### Transfer Reason Decoding

The parser decodes transfer reason bit flags:

```python
if result['transfer_reason_decoded']:
    reason = result['transfer_reason_decoded']

    if reason['first_message_after_reset']:
        print("Device restarted")

    if reason['user_button_triggered']:
        print("Manual transmission triggered")

    retry_count = reason['retry_count']
    triggered_rules = reason['triggered_rules']
```

## Requirements

- Python 3.7+
- No external dependencies (uses only standard library)

## Schema Reference

Based on the ProtoMeasurements protobuf schema:

- Field 1: `bytes serial_num` - Device serial number
- Field 2: `bool battery_status` - Battery health indicator
- Field 3: `uint32 measurement_period_base` - Base measurement interval
- Field 4: `repeated ProtoChannel channels` - Sensor channels
- Field 5: `uint32 next_transmission_at` - Next transmission timestamp
- Field 6: `uint32 transfer_reason` - Transmission reason flags
- Field 8: `uint32 measurement_period_factor` - Period multiplier
- Field 9: `uint32 hash` - Configuration hash
- Field 16: `string cloud_token` - Cloud service token

## License

This parser is based on the ProtoMeasurements schema documentation and protobuf definition provided.
