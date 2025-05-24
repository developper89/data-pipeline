# Pusher Module Implementation Plan

## Overview

The Pusher module will be responsible for sending control commands and settings to IoT devices through the existing connector infrastructure. It will operate by publishing command messages to Kafka topics, which will then be consumed by the appropriate connectors for delivery to devices via MQTT, CoAP, or other protocols.

## Project Structure

```
packages/
└── preservarium-pusher/
    ├── pyproject.toml           # Package definition with dependencies
    ├── README.md                # Documentation
    ├── preservarium_pusher/
    │   ├── __init__.py          # Package initialization with factory function
    │   ├── core/
    │   │   ├── __init__.py
    │   │   ├── models.py        # Data models for device commands
    │   │   ├── config.py        # Configuration settings
    │   │   └── exceptions.py    # Custom exceptions
    │   ├── kafka/
    │   │   ├── __init__.py
    │   │   └── kafka_producer.py # Kafka producer for command messages
    │   └── service.py           # Main service class
    └── tests/
        ├── __init__.py
        ├── test_models.py       # Tests for data models
        └── test_service.py      # Tests for service functionality
```

## Key Components

### 1. Core Models

- `CommandMessage`: Represents a command or setting to be pushed to a device
  - Fields: device_id, command_type, payload, protocol, metadata
- `DeviceCommand`: Specific model for device commands (e.g., set LED state)
- `DeviceSettings`: Model for persistent device settings

### 2. Kafka Integration

- `PusherKafkaProducer`: Wrapper for Kafka producer to handle command publishing
  - Will use shared/mq/kafka_helpers.py for base functionality
  - Publishes to configurable command topics (e.g., "device_commands")

### 3. Main Service

- `PusherService`: Main class that manages the pushing operations
  - Methods for pushing commands and settings
  - Handles initialization and shutdown
  - Manages Kafka connections

## Implementation Steps

1. **Setup Package Structure**

   - Create the directory structure
   - Configure pyproject.toml with dependencies

2. **Create Core Models**

   - Implement CommandMessage based on shared/models/common.py patterns
   - Create protocol-specific command models

3. **Implement Kafka Producer**

   - Create a KafkaProducer wrapper similar to normalizer/kafka_producer.py
   - Integrate with shared Kafka helpers

4. **Build Main Service**

   - Implement the core service functionality
   - Add connection management
   - Create a factory function for service initialization

5. **Add Testing**
   - Write unit tests for models and service

## Integration Points

### Kafka Message Flow

The pusher module will publish command messages to Kafka topics. These messages will then be consumed by the appropriate connectors:

1. `PusherService` creates a `CommandMessage` with protocol-specific details
2. Message is published to a Kafka topic (e.g., "device_commands")
3. MQTT/CoAP connectors consume messages from this topic
4. Connectors translate and deliver commands to devices

### Connector Integration

- No modifications needed to existing connectors if they already consume from Kafka
- If connectors need to be updated, they would need to consume from the new command topics

## Kafka Producer Implementation

The Kafka producer will be similar to the existing normalizer/kafka_producer.py:

```python
# preservarium_pusher/kafka/kafka_producer.py
import logging
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import shared components
from shared.mq.kafka_helpers import publish_message
from preservarium_pusher.core.models import CommandMessage

logger = logging.getLogger(__name__)

class PusherKafkaProducer:
    """
    Wrapper around KafkaProducer for publishing device commands.
    """
    def __init__(self, producer: KafkaProducer, command_topic: str):
        """
        Initialize with a pre-configured KafkaProducer instance.
        """
        self.producer = producer
        self.command_topic = command_topic
        logger.info(f"PusherKafkaProducer initialized with topic {command_topic}")

    def publish_command(self, command: CommandMessage):
        """
        Publish a command message to the command topic.
        """
        if not isinstance(command, CommandMessage):
            raise TypeError("Message must be an instance of CommandMessage")

        try:
            # Use device_id as the key for partitioning
            key = command.device_id
            value = command.model_dump()

            logger.debug(f"[{command.request_id}] Publishing command to topic '{self.command_topic}' with key '{key}'")
            publish_message(self.producer, self.command_topic, value, key)
        except KafkaError as e:
            logger.error(f"[{command.request_id}] Failed to publish command: {e}")
            raise
        except Exception as e:
            logger.exception(f"[{command.request_id}] Unexpected error publishing command: {e}")
            raise

    def close(self, timeout: Optional[float] = None):
        """
        Close the underlying KafkaProducer.
        """
        if self.producer:
            logger.info(f"Closing PusherKafkaProducer (timeout={timeout}s)...")
            try:
                self.producer.close(timeout=timeout)
                logger.info("Producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
            finally:
                self.producer = None
```

## Core Models Example

The command models will follow the pattern used in shared/models/common.py:

```python
# preservarium_pusher/core/models.py
from typing import Any, Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from shared.models.common import CustomBaseModel, generate_request_id

class CommandMessage(CustomBaseModel):
    """
    Base model for device commands.
    """
    device_id: str
    command_type: str  # e.g., "set_led", "reboot"
    payload: Dict[str, Any]  # Command data
    protocol: str  # e.g., "mqtt", "coap"
    topic: Optional[str] = None  # For MQTT
    path: Optional[str] = None  # For CoAP
```

## Usage Examples

### Command via Direct API

```python
from preservarium_pusher import create_pusher_service
from preservarium_pusher.core.models import CommandMessage

# Initialize the service
pusher_service = create_pusher_service(kafka_bootstrap_servers="kafka:9092",
                                       command_topic="device_commands")

# Create and send an MQTT command
command = CommandMessage(
    device_id="device123",
    command_type="set_led",
    payload={"state": "on", "color": "blue"},
    protocol="mqtt",
    topic="devices/device123/commands"
)

# Push the command
await pusher_service.push_command(command)

# Close when done
await pusher_service.close()
```

### Integration with Application

```python
# In an application that needs to send device commands
from preservarium_pusher import create_pusher_service

# Initialize once at application startup
pusher_service = create_pusher_service(
    kafka_bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    command_topic=config.DEVICE_COMMAND_TOPIC
)

# Later, when a command needs to be sent:
async def handle_set_temperature(device_id: str, temperature: float):
    command = {
        "device_id": device_id,
        "command_type": "set_temperature",
        "payload": {"temperature": temperature},
        "protocol": "mqtt",
        "topic": f"devices/{device_id}/settings"
    }
    await pusher_service.push_command_dict(command)
```

## Future Enhancements

- Add support for feedback and acknowledgment tracking
- Implement command queuing and retry mechanisms
- Add telemetry for monitoring command delivery
- Implement command validation based on device capabilities
