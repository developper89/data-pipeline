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
