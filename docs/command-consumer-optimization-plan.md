# Command Consumer Optimization Plan

## Overview

Both `coap_connector/command_consumer.py` and `mqtt_connector/command_consumer.py` contain significant code duplication (~70-80% shared logic). This plan outlines how to refactor the code into a shared base class while maintaining protocol-specific functionality.

## Current Duplication Analysis

### Common Functionality (to be extracted):

1. **AsyncResilientKafkaConsumer integration**

   - Consumer initialization and lifecycle management
   - Message consumption loop with error handling
   - Offset commit logic

2. **Translator management**

   - Translator caching (`_translator_cache`)
   - `_get_translator_for_manufacturer()` method (identical in both)
   - Script module loading logic

3. **Command message processing structure**

   - CommandMessage parsing and validation
   - Basic command filtering by protocol
   - Error handling patterns

4. **Core infrastructure**
   - Start/stop lifecycle methods
   - Consumer error callbacks
   - Pending commands storage structure

### Protocol-Specific Differences:

#### CoAP Connector:

- Returns `bytes` from command formatting
- Stores commands as hex strings in payload
- No publishing logic (commands stored for retrieval)
- No feedback acknowledgment handling

#### MQTT Connector:

- Returns `dict` from command formatting
- Has MQTT publishing logic (topic, qos, retain)
- Feedback acknowledgment processing
- Different payload storage format

## Proposed Solution

### 1. Create Base Class: `BaseCommandConsumer`

**Location:** `shared/consumers/command_consumer_base.py`

**Responsibilities:**

- AsyncResilientKafkaConsumer lifecycle management
- Common message processing pipeline
- Translator caching and retrieval
- Error handling and logging
- Abstract methods for protocol-specific behavior

### 2. Abstract Methods for Protocol-Specific Logic

```python
@abstractmethod
async def format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Any:
    """Format command using protocol-specific parser logic"""
    pass

@abstractmethod
async def store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_data: Any):
    """Store formatted command in protocol-specific format"""
    pass

@abstractmethod
async def handle_protocol_specific_processing(self, command_message: CommandMessage, formatted_data: Any) -> bool:
    """Handle protocol-specific command processing (e.g., MQTT publishing, CoAP storage)"""
    pass

@abstractmethod
def should_process_command(self, command_message: CommandMessage) -> bool:
    """Check if this consumer should process the given command"""
    pass
```

### 3. Refactor Existing Consumers

#### CoAP Command Consumer:

```python
from shared.consumers.command_consumer_base import BaseCommandConsumer

class CommandConsumer(BaseCommandConsumer):
    def __init__(self):
        super().__init__(
            consumer_group_id="iot_command_consumer_coap",
            protocol="coap"
        )

    def should_process_command(self, command_message: CommandMessage) -> bool:
        return command_message.protocol.lower() == 'coap'

    async def format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Optional[bytes]:
        # CoAP-specific formatting logic returning bytes

    async def store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_data: bytes):
        # Store as hex string for CoAP

    async def handle_protocol_specific_processing(self, command_message: CommandMessage, formatted_data: bytes) -> bool:
        # CoAP just stores the command, no additional processing
        return True
```

#### MQTT Command Consumer:

```python
from shared.consumers.command_consumer_base import BaseCommandConsumer

class CommandConsumer(BaseCommandConsumer):
    def __init__(self, mqtt_client: MQTTClientWrapper):
        super().__init__(
            consumer_group_id="iot_command_consumer_mqtt",
            protocol="mqtt"
        )
        self.mqtt_client = mqtt_client

    def should_process_command(self, command_message: CommandMessage) -> bool:
        return command_message.protocol.lower() == 'mqtt'

    async def format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Optional[dict]:
        # MQTT-specific formatting logic returning dict

    async def store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_data: dict):
        # Store dict format for MQTT

    async def handle_protocol_specific_processing(self, command_message: CommandMessage, formatted_data: dict) -> bool:
        # MQTT publishing logic + feedback handling
```

## Implementation Steps

### Phase 1: Create Base Class

1. Create `shared/consumers/` directory
2. Implement `BaseCommandConsumer` with all common functionality
3. Define abstract methods for protocol-specific behavior
4. Add comprehensive error handling and logging

### Phase 2: Refactor CoAP Consumer

1. Update `coap_connector/command_consumer.py` to inherit from base
2. Implement protocol-specific abstract methods
3. Remove duplicated code
4. Test functionality remains identical

### Phase 3: Refactor MQTT Consumer

1. Update `mqtt_connector/command_consumer.py` to inherit from base
2. Implement protocol-specific abstract methods
3. Handle MQTT-specific features (publishing, feedback)
4. Remove duplicated code
5. Test functionality remains identical

### Phase 4: Testing & Validation

1. Unit tests for base class
2. Integration tests for both connectors
3. Ensure Docker container compatibility
4. Verify no breaking changes

## Benefits

### Code Reduction:

- **Before:** ~700 lines total (331 + 378)
- **After:** ~200 lines base + ~100 CoAP + ~150 MQTT = ~450 lines
- **Reduction:** ~35% code reduction, ~70% duplication elimination

### Maintainability:

- Single source of truth for common logic
- Easier to add new protocol connectors
- Consistent error handling across protocols
- Centralized translator management

### Testing:

- Base functionality tested once
- Protocol-specific tests focus on unique behavior
- Better test coverage with less effort

## File Structure After Refactoring

```
shared/
  consumers/
    __init__.py
    command_consumer_base.py    # New base class

connectors/
  coap_connector/
    command_consumer.py         # Simplified, inherits from base
  mqtt_connector/
    command_consumer.py         # Simplified, inherits from base
```

## Compatibility Notes

- Both connectors run in Docker containers sharing `/shared` directory
- No changes to external APIs or message formats
- Maintains existing configuration compatibility
- No changes to Kafka topic structure or message schemas

## Risk Mitigation

1. **Gradual rollout:** Implement and test one connector at a time
2. **Backward compatibility:** Ensure existing functionality is preserved
3. **Thorough testing:** Unit and integration tests before deployment
4. **Rollback plan:** Keep original files until validation complete
