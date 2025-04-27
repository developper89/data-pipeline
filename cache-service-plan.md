# Redis Caching Service Implementation Plan

## Overview

This document outlines the implementation plan for a Redis-based caching service that will consume validated data from the normalizer stage and store only the latest metadata for each device. The metadata comes from the `_build_validated_output` function in `validator.py` and will be cached for quick access.

## Architecture

```
                             ┌─────────────┐
                             │             │
                      ┌─────►│  InfluxDB   │
                      │      │  Ingestor   │
                      │      │             │
                      │      └─────────────┘
┌─────────────┐       │
│             │       │
│ Normalizer  ├───────┤
│             │       │
└─────────────┘       │      ┌─────────────┐
                      │      │             │
                      └─────►│   Redis     │
                             │   Cache     │
                             │   Service   │
                             └─────────────┘
```

## Components

1. **Redis Cache Service**

   - Consumes validated data from Kafka
   - Extracts metadata from ValidatedOutput objects
   - Stores metadata in Redis with TTL
   - Provides an API for other services to query cached metadata

2. **Redis Database**
   - Stores metadata with configurable expiration
   - Provides fast read/write access

## Data Structure Design

### Redis Key Design

We'll use the following key pattern to store device metadata:

- `device:{device_id}:metadata`

### Data Storage Format

**Device Metadata (Hash)**

```
HSET device:{device_id}:metadata
  datatype_id "{datatype_id}"
  datatype_name "{datatype_name}"
  datatype_unit "{datatype_unit}"
  persist "{persist_flag}"
  last_updated "{timestamp}"
```

This structure directly maps to the metadata created in the `_build_validated_output` function:

```python
# From validator.py
metadata = {
    "datatype_id": str(datatype.id) if hasattr(datatype, 'id') else None,
    "datatype_name": datatype.name if hasattr(datatype, 'name') else None,
    "datatype_unit": validation_params.get("unit", None),
    "persist": datatype.persist if hasattr(datatype, 'persist') else True,
}
```

We'll also keep a set of all device IDs for quickly listing available devices:

```
SADD devices {device_id}
```

## Implementation Details

### Redis Cache Service Structure

```
cache_service/
├── Dockerfile
├── docker-entrypoint.sh
├── requirements.txt
├── main.py
├── service.py
├── config.py
├── redis_client.py
└── metadata_cache.py
```

### Key Components

1. **Redis Client**

   - Handles connection pool management
   - Provides methods for metadata storage and retrieval
   - Implements retry logic and error handling

2. **Metadata Cache**

   - Extracts metadata from ValidatedOutput
   - Updates Redis with new metadata
   - Sets appropriate TTL for cached data

3. **Cache Service**
   - Consumes messages from Kafka
   - Orchestrates caching operations
   - Manages lifecycle and error handling

### Configuration Parameters

```python
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_VALIDATED_DATA_TOPIC = os.getenv("KAFKA_VALIDATED_DATA_TOPIC", "iot_validated_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "cache_service_group")
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_METADATA_TTL = int(os.getenv("REDIS_METADATA_TTL", "86400"))  # 24 hours

# Service Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
```

### Docker Integration

Update `docker-compose.yml` to include:

```yaml
redis:
  image: redis:7-alpine
  container_name: redis
  ports:
    - "6379:6379"
  volumes:
    - redis-data:/data
  command: ["redis-server", "--appendonly", "yes"]
  networks:
    - preservarium_net

cache_service:
  container_name: cache_service
  build:
    context: .
    dockerfile: cache_service/Dockerfile
  volumes:
    - ./cache_service:/app/cache_service
    - ./shared:/app/shared
  environment:
    # Kafka Configuration
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    - KAFKA_VALIDATED_DATA_TOPIC=iot_validated_data
    - KAFKA_ERROR_TOPIC=iot_errors
    - KAFKA_CONSUMER_GROUP_ID=cache_service_group
    - KAFKA_CONSUMER_POLL_TIMEOUT_S=1.0
    # Redis Configuration
    - REDIS_HOST=redis
    - REDIS_PORT=6379
    - REDIS_DB=0
    - REDIS_PASSWORD=
    - REDIS_METADATA_TTL=86400
    # Service Configuration
    - LOG_LEVEL=INFO
  networks:
    - preservarium_net
  depends_on:
    - kafka
    - redis
```

### Error Handling Strategy

1. **Kafka Consumption Errors**

   - Implement retry logic for transient errors
   - Log persistent errors and continue processing

2. **Redis Connection Errors**

   - Implement connection pooling with automatic reconnection
   - Use exponential backoff for retries

3. **Data Processing Errors**
   - Log invalid data formats
   - Continue processing valid messages

## Implementation Steps

### Phase 1: Core Service Setup

1. Create directory structure
2. Implement configuration file
3. Setup basic Redis client
4. Create Kafka consumer setup

### Phase 2: Data Processing Implementation

1. Implement metadata extraction logic
2. Develop Redis storage strategies
3. Implement TTL handling

### Phase 3: Docker and Integration

1. Create Dockerfile
2. Update docker-compose.yml
3. Setup networking
4. Configure environment variables

## Enhancement Possibilities

1. **Simple REST API**

   - Add a basic endpoint to retrieve metadata for a device
   - Make cached data accessible to external services

2. **Bulk Device Lookup**
   - Support looking up multiple devices in a single request
   - Implement pipelining for efficient Redis operations

## Conclusion

This Redis caching service will provide fast access to device metadata by storing the metadata extracted from the validator stage. The implementation focuses solely on caching the metadata fields from the `_build_validated_output` function, ensuring that the most up-to-date device information is always available without having to query slower backend systems.
