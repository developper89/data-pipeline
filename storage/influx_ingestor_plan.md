# InfluxDB Ingestor Implementation Plan

## Overview

This document outlines the plan for implementing Stage 3 of the data pipeline: the InfluxDB ingestor. This component will consume validated data from Stage 2 (normalizer), and ingest it into InfluxDB 2.0 for time series storage and analysis.

## Analysis of Existing Stages

### Stage 1: Connectors

- Located in `connectors/` directory
- Protocol-specific subdirectories (mqtt_connector, coap_connector)
- Each connector has:
  - `main.py`: Entry point
  - `client.py`: Protocol-specific client implementation
  - `kafka_producer.py`: For sending data to Kafka
  - `config.py`: Configuration handling
  - Dockerfile and requirements.txt
  - Scripts for container setup (entrypoint.sh)

### Stage 2: Normalizer

- Located in `normalizer/` directory
- Key components:
  - `main.py`: Entry point with lifecycle management
  - `service.py`: Core service implementation
  - `validator.py`: Data validation logic
  - `kafka_producer.py`: For sending validated data to Kafka
  - `config.py`: Configuration handling
  - Database integration with PostgreSQL

## Stage 3: InfluxDB Ingestor Plan

### Directory Structure

```
ingestor/
├── Dockerfile
├── docker-entrypoint.sh
├── requirements.txt
├── main.py            # Entry point with lifecycle management
├── service.py         # Core ingestor service implementation
├── config.py          # Configuration handling
├── influx_writer.py   # InfluxDB client/writer implementation
└── data_mapper.py     # Maps normalized data to InfluxDB points
```

### Key Components

1. **Kafka Consumer**:

   - Consumes validated data from the "iot_validated_data" Kafka topic
   - Handles message deserialization and error handling

2. **Data Mapper**:

   - Maps normalized data to InfluxDB data points
   - Handles field mappings, tags, and measurements
   - Transforms data structure as needed for time series storage

3. **InfluxDB Writer**:

   - Manages connection to InfluxDB 2.0
   - Handles authentication and bucket management
   - Provides batch writing capabilities with retry logic

4. **Main Service**:
   - Manages lifecycle and orchestrates components
   - Provides graceful startup/shutdown
   - Implements error handling and logging

### Configuration Parameters

The ingestor will require the following configuration parameters:

```python
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_VALIDATED_DATA_TOPIC = os.getenv("KAFKA_VALIDATED_DATA_TOPIC", "iot_validated_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "ingestor_group")
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "preservarium")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")
INFLUXDB_BATCH_SIZE = int(os.getenv("INFLUXDB_BATCH_SIZE", "100"))
INFLUXDB_FLUSH_INTERVAL_MS = int(os.getenv("INFLUXDB_FLUSH_INTERVAL_MS", "5000"))

# Service Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
```

### Docker Integration

1. Update `docker-compose.yml` to add:

   - InfluxDB service (if not using external instance)
   - Ingestor service with appropriate environment variables
   - Network configuration for communication with Kafka and InfluxDB

2. Dockerfile for the ingestor:
   - Based on Python 3.9+ image
   - Installs required dependencies (influxdb-client, kafka-python, etc.)
   - Sets up entrypoint script

### Implementation Steps

1. Create directory structure
2. Implement core components:
   - Kafka consumer setup with proper error handling
   - InfluxDB client with connection management
   - Data mapping logic
   - Main service with lifecycle management
3. Add Docker configuration
4. Implement logging and metrics
5. Add error handling, retry logic, and monitoring
6. Test with sample data flows
7. Integrate with existing stages

### Testing Plan

1. Unit tests for individual components
2. Integration tests for data flow through the pipeline
3. Performance testing with varying data volumes
4. Failure recovery testing

### Future Enhancements

1. Dashboard integration for monitoring
2. Alerting on ingest failures
3. Data backfill capabilities
4. Schema evolution handling
