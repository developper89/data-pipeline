# Python Client Library for Cache Service

## Overview

This document outlines the plan for implementing a Python client library for interacting with the Redis cache service. This library will provide a type-safe, well-documented API that allows other Python applications to easily access cached device metadata.

## Current State Analysis

The existing `caching` directory contains:

- `redis_client.py`: Redis connection and basic operations
- `metadata_cache.py`: Logic for caching device metadata
- `service.py`: Kafka consumer that processes messages and caches data
- `config.py`: Configuration loading from environment variables
- `main.py`: Service startup and lifecycle management
- Docker-related files for deployment

The current implementation is focused on running as a standalone service that consumes Kafka messages. To convert it into a reusable library, we need to:

1. Decouple the Redis client from the service components
2. Create proper package structure with proper imports
3. Make configuration more flexible for library users
4. Implement a clean public API
5. Add proper type hints and documentation

## Purpose

The Python client library serves several key purposes:

- Provide a clean, Pythonic interface for accessing the cache service
- Abstract away Redis implementation details
- Offer type-safe models with proper validation
- Enable seamless integration with other Python services
- Reduce code duplication across projects that need to access the cache

## Technical Architecture

### Package Structure

```
cache_service_client/
├── __init__.py          # Package exports and version
├── client.py            # Main client class (public API)
├── models.py            # Pydantic models for types
├── exceptions.py        # Custom exceptions
├── utils.py             # Helper utilities
├── config.py            # Configuration handling
└── internal/
    ├── __init__.py
    ├── redis_client.py  # Refactored from existing redis_client.py
    └── metadata_cache.py # Refactored from existing metadata_cache.py
```

### Technology Stack

- **Pydantic**: For data validation and serialization
- **Redis-py**: For direct Redis interaction
- **Httpx**: For REST API interaction (optional)
- **Backoff**: For retry logic
- **Typing**: For comprehensive type annotations

## Required Changes

### 1. Refactor Redis Client

Current limitations:

- Directly imports from `config.py`
- Uses global logger configuration
- Limited error handling and retry options

Changes needed:

- Make configuration parameters constructor arguments with defaults
- Implement proper connection pool management
- Add support for async operations
- Create clear exception hierarchy
- Add batch operations support

### 2. Refactor Metadata Cache

Current limitations:

- Depends on `ValidatedOutput` from shared models
- Tied to specific cache key structure
- Limited query options

Changes needed:

- Create standalone models without dependencies on `shared` package
- Make key structure configurable
- Add more flexible query options (filtering, sorting)
- Add support for async operations

### 3. Create Public Client Interface

```python
class CacheServiceClient:
    """Client for interacting with the Cache Service."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        socket_timeout: float = 5.0,
        connection_pool: Optional[redis.ConnectionPool] = None,
        metadata_ttl: int = 86400,  # 24 hours
        use_http: bool = False,
        http_url: Optional[str] = None,
        http_timeout: float = 10.0,
        http_auth_token: Optional[str] = None
    ):
        """Initialize the cache service client."""

    async def get_device_metadata(self, device_id: str) -> DeviceMetadata:
        """Retrieve metadata for a specific device."""

    async def list_devices(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        pattern: Optional[str] = None
    ) -> List[str]:
        """List all device IDs in the cache."""

    async def get_cache_info(self) -> CacheInfo:
        """Get information about the cache state."""

    async def check_health(self) -> HealthStatus:
        """Check if the cache service is healthy."""

    def store_device_metadata(self, device_id: str, metadata: Dict[str, Any]) -> bool:
        """Store metadata for a specific device."""

    def close(self) -> None:
        """Close all connections."""

    # Synchronous versions of async methods
    def get_device_metadata_sync(self, device_id: str) -> DeviceMetadata:
        """Synchronous version of get_device_metadata."""

    def list_devices_sync(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        pattern: Optional[str] = None
    ) -> List[str]:
        """Synchronous version of list_devices."""
```

### 4. Create Data Models

Key models to implement:

```python
class DeviceMetadata(BaseModel):
    """Model for device metadata."""
    device_id: str
    datatype_id: str
    datatype_name: str
    datatype_unit: Optional[str]
    persist: bool
    last_updated: datetime
    additional_properties: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        extra = "allow"  # Allow additional fields

class CacheInfo(BaseModel):
    """Model for cache state information."""
    total_devices: int
    memory_usage_bytes: int
    oldest_entry_timestamp: Optional[datetime]
    newest_entry_timestamp: Optional[datetime]

class HealthStatus(BaseModel):
    """Model for service health status."""
    status: Literal["healthy", "degraded", "unhealthy"]
    message: Optional[str]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
```

### 5. Configuration Management

Current limitations:

- Entirely based on environment variables
- No configuration overrides
- No validation of configuration values

Changes needed:

- Support multiple configuration sources (constructor, env vars, config file)
- Validate configuration values
- Allow runtime configuration changes
- Add support for connection strings

## Implementation Plan

### Phase 1: Core Restructuring

1. Create proper package structure
2. Move existing code to internal directory
3. Refactor Redis client to accept configuration as parameters
4. Create basic client class that exposes Redis functionality
5. Implement proper error handling

### Phase 2: API Development

1. Create Pydantic models for all data structures
2. Implement synchronous and asynchronous APIs
3. Add connection pooling
4. Implement advanced query features
5. Add batch operations support

### Phase 3: Testing and Documentation

1. Create comprehensive unit tests
2. Set up integration tests with Redis
3. Create API documentation
4. Write usage examples
5. Prepare package for PyPI

## Migration Guide

For users of the existing cache service:

1. Install the new library:

   ```bash
   pip install cache-service-client
   ```

2. Replace direct Redis client usage:

   Old:

   ```python
   from caching.redis_client import RedisClient

   client = RedisClient()
   metadata = client.get_device_metadata("device_123")
   ```

   New:

   ```python
   from cache_service_client import CacheServiceClient

   client = CacheServiceClient(
       host="redis.example.com",
       port=6379,
       password="secure-password"
   )
   metadata = client.get_device_metadata_sync("device_123")
   ```

3. For async applications:
   ```python
   async with CacheServiceClient(...) as client:
       metadata = await client.get_device_metadata("device_123")
   ```

## Distribution

The library will be packaged for easy installation:

1. **PyPI Package**

   ```bash
   pip install cache-service-client
   ```

2. **Source Distribution**
   Available via Git repository for direct installation or customization.

3. **Documentation**
   Hosted on Read the Docs with comprehensive guides and API references.

## Conclusion

Converting the existing caching service into a proper Python library will require significant refactoring but will yield a much more reusable and flexible component. The changes outlined in this plan will transform the service-oriented code into a clean, well-documented library that can be used by multiple applications while maintaining the core functionality of the original service.
