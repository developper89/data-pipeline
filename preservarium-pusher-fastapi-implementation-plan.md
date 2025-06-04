# Preservarium Pusher FastAPI Implementation Plan

## Overview

Convert the preservarium-pusher from a Python module to a FastAPI web service that provides HTTP endpoints for pushing commands to IoT devices. This will enable applications to send commands via HTTP requests instead of importing and using a Python library directly.

## Current Architecture vs New Architecture

### **Current (Python Module):**

```
Application Code → PusherService → Kafka → Connectors → Devices
```

### **New (FastAPI Service):**

```
Application → HTTP API → FastAPI Service → Kafka → Connectors → Devices
```

## Implementation Plan

### 1. Project Structure

```
packages/preservarium-pusher-api/
├── preservarium_pusher_api/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── api/
│   │   ├── __init__.py
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── commands.py     # Command endpoints
│   │   │   └── health.py       # Health check endpoints
│   │   └── dependencies.py     # FastAPI dependencies
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py          # Application configuration
│   │   ├── models.py          # Pydantic models (reuse existing)
│   │   └── exceptions.py      # Custom exceptions
│   ├── services/
│   │   ├── __init__.py
│   │   ├── pusher_service.py  # Business logic (adapted from existing)
│   │   └── kafka_service.py   # Kafka operations
│   └── utils/
│       ├── __init__.py
│       └── logging.py         # Logging configuration
├── tests/
│   ├── __init__.py
│   ├── api/
│   │   └── test_commands.py
│   └── services/
│       └── test_pusher_service.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
└── README.md
```

### 2. API Endpoints Design

#### **2.1 Command Endpoints**

```yaml
POST /api/v1/commands
  Description: Create and push a single command
  Request Body: CommandRequest (based on existing CommandMessage)
  Response: CommandResponse with status and request_id

POST /api/v1/commands/batch
  Description: Create and push multiple commands
  Request Body: BatchCommandRequest (array of CommandRequest)
  Response: BatchCommandResponse with individual results

GET /api/v1/commands/{request_id}/status
  Description: Get status of a previously submitted command
  Response: CommandStatusResponse
```

#### **2.2 Health and Status Endpoints**

```yaml
GET /health
  Description: Basic health check
  Response: {"status": "healthy"}

GET /api/v1/status
  Description: Detailed service status
  Response: Service status, Kafka connection, metrics

GET /api/v1/metrics
  Description: Service metrics (commands sent, errors, etc.)
  Response: Metrics data
```

### 3. Request/Response Models

#### **3.1 Command Request Model**

```python
class CommandRequest(BaseModel):
    device_id: str
    command_type: str
    payload: Dict[str, Any]
    protocol: str
    parser_script_ref: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    qos: Optional[int] = None
    retain: Optional[bool] = None
    confirmable: Optional[bool] = None

    class Config:
        schema_extra = {
            "example": {
                "device_id": "efento_device_001",
                "command_type": "config_update",
                "payload": {"measurement_interval": 600},
                "protocol": "coap",
                "parser_script_ref": "efento_bidirectional_parser.py"
            }
        }
```

#### **3.2 Response Models**

```python
class CommandResponse(BaseModel):
    success: bool
    request_id: str
    message: str
    timestamp: datetime

class BatchCommandRequest(BaseModel):
    commands: List[CommandRequest]

class BatchCommandResponse(BaseModel):
    total_commands: int
    successful: int
    failed: int
    results: List[CommandResponse]

class ServiceStatus(BaseModel):
    service: str
    version: str
    kafka_connected: bool
    uptime: str
    commands_processed: int
    last_error: Optional[str] = None
```

### 4. Service Layer Implementation

#### **4.1 Pusher Service (Adapted)**

```python
class PusherService:
    def __init__(self, kafka_bootstrap_servers: str, command_topic: str):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.command_topic = command_topic
        self.kafka_producer = None
        self._metrics = {
            "commands_sent": 0,
            "commands_failed": 0,
            "last_error": None
        }

    async def push_command(self, command_request: CommandRequest) -> CommandResponse:
        """Convert CommandRequest to CommandMessage and push to Kafka"""

    async def push_commands_batch(self, commands: List[CommandRequest]) -> BatchCommandResponse:
        """Push multiple commands in batch"""

    def get_metrics(self) -> Dict[str, Any]:
        """Get service metrics"""

    async def health_check(self) -> bool:
        """Check if service is healthy (Kafka connection, etc.)"""
```

#### **4.2 Configuration Management**

```python
class Settings(BaseSettings):
    # Kafka Configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_command_topic: str = "device_commands"
    kafka_producer_timeout: int = 30

    # API Configuration
    api_title: str = "Preservarium Pusher API"
    api_version: str = "1.0.0"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Logging
    log_level: str = "INFO"

    # Security (future)
    api_key_header: str = "X-API-Key"
    enable_auth: bool = False

    class Config:
        env_file = ".env"
```

### 5. FastAPI Application Structure

#### **5.1 Main Application (main.py)**

```python
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from preservarium_pusher_api.core.config import get_settings
from preservarium_pusher_api.api.v1 import commands, health
from preservarium_pusher_api.services.pusher_service import PusherService

def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description="API for pushing commands to IoT devices"
    )

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(health.router, tags=["health"])
    app.include_router(commands.router, prefix="/api/v1", tags=["commands"])

    return app

app = create_app()

@app.on_event("startup")
async def startup_event():
    # Initialize PusherService
    settings = get_settings()
    app.state.pusher_service = PusherService(
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        command_topic=settings.kafka_command_topic
    )
    await app.state.pusher_service.initialize()

@app.on_event("shutdown")
async def shutdown_event():
    # Clean up resources
    if hasattr(app.state, 'pusher_service'):
        await app.state.pusher_service.close()

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True  # Only for development
    )
```

#### **5.2 Command Endpoints (api/v1/commands.py)**

```python
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List

from preservarium_pusher_api.core.models import (
    CommandRequest, CommandResponse,
    BatchCommandRequest, BatchCommandResponse
)
from preservarium_pusher_api.services.pusher_service import PusherService

router = APIRouter()

def get_pusher_service(request: Request) -> PusherService:
    return request.app.state.pusher_service

@router.post("/commands", response_model=CommandResponse)
async def push_command(
    command: CommandRequest,
    pusher_service: PusherService = Depends(get_pusher_service)
):
    """Push a single command to a device."""
    try:
        result = await pusher_service.push_command(command)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/commands/batch", response_model=BatchCommandResponse)
async def push_commands_batch(
    batch_request: BatchCommandRequest,
    pusher_service: PusherService = Depends(get_pusher_service)
):
    """Push multiple commands to devices."""
    try:
        result = await pusher_service.push_commands_batch(batch_request.commands)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status", response_model=dict)
async def get_service_status(
    pusher_service: PusherService = Depends(get_pusher_service)
):
    """Get detailed service status and metrics."""
    try:
        return {
            "service": "preservarium-pusher-api",
            "kafka_connected": await pusher_service.health_check(),
            "metrics": pusher_service.get_metrics()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

### 6. Error Handling

#### **6.1 Custom Exceptions**

```python
class PusherAPIException(Exception):
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code

class KafkaConnectionError(PusherAPIException):
    def __init__(self, message: str = "Failed to connect to Kafka"):
        super().__init__(message, 503)

class CommandValidationError(PusherAPIException):
    def __init__(self, message: str):
        super().__init__(message, 400)
```

#### **6.2 Exception Handlers**

```python
@app.exception_handler(PusherAPIException)
async def pusher_exception_handler(request: Request, exc: PusherAPIException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message, "type": type(exc).__name__}
    )
```

### 7. Deployment Configuration

#### **7.1 Dockerfile**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Install the package
RUN pip install -e .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "preservarium_pusher_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### **7.2 Docker Compose**

```yaml
version: "3.8"
services:
  preservarium-pusher-api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_COMMAND_TOPIC=device_commands
      - LOG_LEVEL=INFO
    depends_on:
      - kafka
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### 8. API Usage Examples

#### **8.1 Single Command**

```bash
curl -X POST "http://localhost:8000/api/v1/commands" \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "efento_device_001",
    "command_type": "config_update",
    "payload": {"measurement_interval": 600},
    "protocol": "coap",
    "parser_script_ref": "efento_bidirectional_parser.py"
  }'
```

#### **8.2 Batch Commands**

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
        "parser_script_ref": "device_parser.py"
      },
      {
        "device_id": "device_002",
        "command_type": "update",
        "payload": {"version": "1.2.3"},
        "protocol": "mqtt",
        "parser_script_ref": "mqtt_parser.py",
        "metadata": {"topic": "devices/device_002/commands"}
      }
    ]
  }'
```

#### **8.3 Python Client Example**

```python
import httpx
import asyncio

async def push_command():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8000/api/v1/commands",
            json={
                "device_id": "efento_device_001",
                "command_type": "config_update",
                "payload": {"measurement_interval": 600},
                "protocol": "coap",
                "parser_script_ref": "efento_bidirectional_parser.py"
            }
        )
        return response.json()

result = asyncio.run(push_command())
print(result)
```

### 9. Testing Strategy

#### **9.1 Unit Tests**

```python
def test_push_command_success():
    # Test successful command pushing

def test_push_command_invalid_data():
    # Test validation errors

def test_kafka_connection_failure():
    # Test Kafka connection errors
```

#### **9.2 Integration Tests**

```python
@pytest.mark.asyncio
async def test_api_endpoint_push_command():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/api/v1/commands", json={...})
        assert response.status_code == 200
```

### 10. Migration Considerations

#### **10.1 Backwards Compatibility**

- Keep the existing Python module for applications that can't migrate immediately
- Provide a client library that wraps HTTP calls to maintain similar interface
- Document migration path from module to API

#### **10.2 Client Library (Optional)**

```python
class PusherAPIClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url
        self.api_key = api_key

    async def push_command(self, **kwargs) -> bool:
        """Wrapper to maintain compatibility with existing code"""
        # Make HTTP request to API
        # Return boolean for compatibility
```

### 11. Security Considerations

#### **11.1 Authentication (Future Enhancement)**

```python
# API Key authentication
@router.post("/commands")
async def push_command(
    command: CommandRequest,
    api_key: str = Depends(get_api_key),
    pusher_service: PusherService = Depends(get_pusher_service)
):
```

#### **11.2 Rate Limiting**

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@router.post("/commands")
@limiter.limit("100/minute")  # 100 requests per minute
async def push_command(...):
```

### 12. Monitoring and Observability

#### **12.1 Metrics Collection**

```python
# Prometheus metrics
from prometheus_client import Counter, Histogram

COMMANDS_TOTAL = Counter('commands_total', 'Total commands processed')
COMMAND_DURATION = Histogram('command_duration_seconds', 'Command processing time')
```

#### **12.2 Logging**

```python
# Structured logging
import structlog

logger = structlog.get_logger()

await logger.info(
    "Command processed",
    device_id=command.device_id,
    command_type=command.command_type,
    request_id=request_id
)
```

### 13. Implementation Timeline

#### **Phase 1: Core API (Week 1)**

- Basic FastAPI application structure
- Single command endpoint
- Basic error handling
- Health check endpoint

#### **Phase 2: Enhanced Features (Week 2)**

- Batch command endpoint
- Comprehensive error handling
- Service status and metrics
- Docker deployment

#### **Phase 3: Production Ready (Week 3)**

- Comprehensive testing
- Documentation
- Security considerations
- Performance optimization

#### **Phase 4: Migration Support (Week 4)**

- Client library for backwards compatibility
- Migration documentation
- Deployment guides

## Benefits of FastAPI Implementation

1. **Language Agnostic**: Any application can use HTTP API regardless of programming language
2. **Scalability**: Can be deployed as multiple instances behind load balancer
3. **Monitoring**: Better observability with HTTP metrics and health checks
4. **Documentation**: Auto-generated OpenAPI/Swagger documentation
5. **Security**: Centralized authentication and authorization
6. **Decoupling**: Applications don't need to import Python dependencies
7. **Container-First**: Easy to deploy in Kubernetes/Docker environments

## Questions for Approval

1. Do you want to maintain the existing Python module alongside the API?
2. Should we implement authentication from the start or add it later?
3. Do you want real-time status tracking for commands (with database persistence)?
4. Should we include rate limiting in the initial implementation?
5. Any specific monitoring/metrics requirements?

Please review and approve this plan, and I'll start implementing the FastAPI service.
