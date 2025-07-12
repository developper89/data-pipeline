# Kafka Connection Improvements Guide

## Overview

The `shared/mq/kafka_helpers.py` has been enhanced to automatically handle the common Kafka connection issues you were experiencing:

- Heartbeat session expired
- Request timeouts
- Metadata retrieval failures
- Connection drops

## Automatic Improvements

### All Services Get These Benefits Automatically:

✅ **Increased Timeouts** - Better handling of network delays
✅ **Smarter Reconnection** - Exponential backoff with longer retry periods
✅ **Health Monitoring** - Automatic detection of connection issues
✅ **Optimized Configuration** - Better performance and reliability

## Error Types Fixed

### 1. Heartbeat Session Expired

**Before:**

```
Heartbeat session expired, marking coordinator dead
```

**Fixed By:**

- Increased `session_timeout_ms` to 90 seconds
- Increased `heartbeat_interval_ms` to 30 seconds
- Better reconnection logic

### 2. Request Timeouts

**Before:**

```
Request timed out after 40000.0 ms
```

**Fixed By:**

- Increased `request_timeout_ms` to 120 seconds
- Added better retry logic
- Improved connection pool settings

### 3. Metadata Issues

**Before:**

```
No broker metadata found in MetadataResponse
```

**Fixed By:**

- Increased `metadata_max_age_ms` to 60 seconds
- Better API version detection timeout
- Enhanced broker validation

## Optional Service Enhancements

### Option 1: Use Safe Utility Functions (Recommended)

Replace your polling and commit logic with safer alternatives:

```python
# Before
message_batch = consumer.poll(timeout_ms=1000)
consumer.commit()

# After
from shared.mq.kafka_helpers import safe_kafka_poll, safe_kafka_commit

message_batch = safe_kafka_poll(consumer, timeout_ms=1000)
safe_kafka_commit(consumer)
```

### Option 2: Use ResilientKafkaConsumer (Advanced)

For services that need maximum reliability:

```python
from shared.mq.kafka_helpers import ResilientKafkaConsumer

# Replace your consumer creation and polling loop
resilient_consumer = ResilientKafkaConsumer(
    topic="your-topic",
    group_id="your-group",
    bootstrap_servers="kafka:9092",
    on_error_callback=lambda error, message: logger.error(f"Error: {error}")
)

def process_message(message):
    # Your message processing logic
    pass

# This handles all the retry logic, health checks, and reconnections
resilient_consumer.consume_messages(process_message)
```

### Option 3: Add Health Checks

Monitor your consumer health:

```python
from shared.mq.kafka_helpers import is_consumer_healthy

if not is_consumer_healthy(consumer):
    logger.warning("Consumer unhealthy, recreating...")
    consumer = recreate_consumer_on_error(consumer, topic, group_id, bootstrap_servers)
```

## Testing Your Improvements

Use the new diagnostics tool to verify your setup:

```bash
# Run comprehensive diagnostics
python tools/kafka_diagnostics.py --bootstrap-servers kafka:9092 --topic test-topic --group-id test-group

# Test specific consumer group stability
python tools/kafka_diagnostics.py --bootstrap-servers kafka:9092 --group-id coap_command_consumer --test-duration 60
```

## Configuration Overrides

You can still override specific settings when needed:

```python
# Custom consumer configuration
consumer = create_kafka_consumer(
    topic="my-topic",
    group_id="my-group",
    bootstrap_servers="kafka:9092",
    session_timeout_ms=60000,  # Override default
    heartbeat_interval_ms=20000  # Override default
)
```

## Monitoring

The enhanced helpers provide better logging. Look for these log messages:

- ✅ `KafkaConsumer connected to kafka:9092 for topic 'topic', group 'group'`
- ⚠️ `Consumer health check failed for topic 'topic', triggering reconnection`
- 🔄 `Successfully recreated consumer after error`

## No Action Required

Your existing services will automatically benefit from these improvements without any code changes!
