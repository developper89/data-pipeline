# Cache Service Interaction Plan

## Overview

This document outlines the plan for implementing interfaces to interact with the Redis cache service, allowing users and other services to retrieve cached device metadata.

## Current State

The cache service currently:

- Consumes validated data from Kafka
- Stores device metadata in Redis
- Maintains the latest metadata for each device
- Doesn't provide any external interfaces for data retrieval

## Interface Options

We propose implementing the following interfaces for accessing cached data:

### 1. REST API

A lightweight REST API providing endpoints to retrieve device metadata.

#### Benefits:

- Language-agnostic access via HTTP
- Easy integration with web applications and dashboards
- Familiar request/response pattern for developers
- Can add authentication and rate limiting

#### Implementation Details:

```
cache_service/
└── api/
    ├── __init__.py
    ├── app.py        # FastAPI application
    ├── routes.py     # API route handlers
    └── models.py     # API request/response models
```

#### Endpoints:

- `GET /api/devices` - List all devices in cache
- `GET /api/devices/{device_id}` - Get metadata for a specific device
- `GET /health` - Service health check

### 2. CLI Command Tool

A command-line interface to query the cache directly.

#### Benefits:

- Easy for operations teams to use
- Useful for scripts and automation
- No need for separate HTTP requests

#### Implementation Details:

```
cache_service/
└── cli/
    ├── __init__.py
    ├── commands.py   # CLI command implementations
    └── main.py       # CLI entry point
```

#### Commands:

- `cache-cli list` - List all devices in cache
- `cache-cli get <device_id>` - Get metadata for a device
- `cache-cli info` - Show cache statistics

### 3. Python Client Library

A Python client library for applications that need to access the cache.

#### Benefits:

- Native Python integration
- Type hints and validation
- More efficient than HTTP for internal services

#### Implementation Details:

```
cache_service/
└── client/
    ├── __init__.py
    ├── client.py     # Client class implementation
    └── models.py     # Data models
```

#### Usage Example:

```python
from cache_service.client import CacheClient

client = CacheClient(redis_host="redis", redis_port=6379, redis_password="password")
metadata = client.get_device_metadata("device123")
```

## Implementation Priority

We recommend implementing these interfaces in the following order:

1. **REST API** (High Priority)

   - Most versatile option
   - Can be used by both internal services and external users
   - Easiest to secure and monitor

2. **Python Client Library** (Medium Priority)

   - Useful for tight integration with other Python services
   - Builds on the same core functionality as the REST API

3. **CLI Tool** (Lower Priority)
   - Useful for operations but less critical for service functionality
   - Can be implemented after the API is stable

## REST API Implementation Plan

### Phase 1: Core API Setup

1. Add FastAPI to requirements.txt
2. Create basic API structure
3. Implement device listing endpoint
4. Implement single device metadata endpoint
5. Add basic error handling

### Phase 2: API Enhancement

1. Add authentication (optional)
2. Add rate limiting
3. Implement pagination for device listings
4. Add filtering options
5. Implement API documentation with Swagger/OpenAPI

### Phase 3: Monitoring and Metrics

1. Add request logging
2. Implement metrics collection (request count, response time)
3. Add cache statistics endpoint

## Docker Integration

Update docker-compose.yml to expose the API port:

```yaml
cache_service:
  container_name: cache_service
  # ... existing configuration ...
  ports:
    - "8080:8080" # Expose API port
  environment:
    # ... existing environment variables ...
    - API_ENABLED=true
    - API_HOST=0.0.0.0
    - API_PORT=8080
```

## Security Considerations

1. **Authentication**: Implement token-based authentication for the API
2. **Rate Limiting**: Protect against excessive requests
3. **Input Validation**: Sanitize all user inputs
4. **Logging**: Ensure sensitive data is not logged

## Conclusion

Implementing these interfaces will make the cache service more useful and accessible to other components of the system. The REST API provides the most immediate value with the broadest compatibility, while the Python client and CLI tool offer more specialized integration options.
