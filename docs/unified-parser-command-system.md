# Unified Parser-Based Command System

## Overview

This document outlines a unified approach where all manufacturer-specific logic (both incoming message parsing and outgoing command formatting) is centralized in parser files. This eliminates the need for field mappings in configuration and separate formatter classes.

## Current Architecture Analysis

### Incoming Message Flow

```
Raw Message → Normalizer Service → Parser Script → parse() → Standardized Output
```

### Proposed Outgoing Command Flow

```
Command Message → Command Consumer → Parser Script → format_command() → Device Binary
```

## Key Principles

1. **Single Source of Truth**: Each manufacturer's parser file contains ALL device-specific logic
2. **Consistent Interface**: Both `parse()` and `format_command()` use the same signature pattern
3. **Translator Integration**: Existing translator instances handle protobuf encoding/decoding
4. **No Configuration Sprawl**: Remove field_mappings and manufacturer-specific config sections

## Implementation Plan

### Phase 1: Enhanced Parser Interface

#### 1.1 Update Parser File Contract

All parser files (like `coap_parser.py`) must implement:

```python
def parse(payload: bytes, translator: Any, metadata: dict, config: dict) -> List[dict]:
    """Parse incoming device messages (EXISTING)"""
    pass

def format_command(command: dict, translator: Any, config: dict) -> bytes:
    """Format outgoing commands to device binary format (ENHANCED)"""
    pass
```

#### 1.2 Enhanced format_command Implementation

```python
def format_command(command: dict, translator: Any, config: dict) -> bytes:
    """
    Format command for Efento devices using protobuf translator.

    Args:
        command: StandardizedCommand dict with fields:
            - command_type: "alarm", "config", "firmware_update", etc.
            - device_id: Target device identifier
            - payload: Command-specific data
            - metadata: Additional command context
        translator: ProtobufTranslator instance for this manufacturer
        config: Hardware configuration dict

    Returns:
        bytes: Binary protobuf message ready for CoAP transmission
    """
    command_type = command.get('command_type')
    payload = command.get('payload', {})
    device_id = command.get('device_id')

    if command_type == "alarm":
        return _format_alarm_command(payload, translator, config)
    elif command_type == "config":
        return _format_config_command(payload, translator, config)
    elif command_type == "firmware_update":
        return _format_firmware_command(payload, translator, config)
    else:
        raise ValueError(f"Unsupported command type: {command_type}")

def _format_alarm_command(alarm_data: dict, translator: Any, config: dict) -> bytes:
    """Convert alarm data to Efento ProtoConfig with embedded rules."""

    # Create Efento-specific rule structure
    efento_rule = {
        'channel_mask': _calculate_channel_mask(alarm_data.get('channel', 1)),
        'condition': _map_condition(alarm_data.get('operator')),
        'parameters': _encode_parameters(alarm_data),
        'action': _map_action(alarm_data.get('level', 'info'))
    }

    # Create ProtoConfig with embedded rule
    proto_config = {
        'measurement_period_base': config.get('measurement_period_base', 60),
        'measurement_period_factor': config.get('measurement_period_factor', 1),
        'rules': [efento_rule]  # Add rule to rules array
    }

    # Use translator's message_parser to encode dictionary to protobuf bytes
    return translator.message_parser.parse_dict_to_message(
        data=proto_config,
        message_type="config"  # Maps to ProtoConfig
    )

def _calculate_channel_mask(channel: int) -> int:
    """Convert channel number to Efento channel mask."""
    return 1 << (channel - 1)  # Channel 1 = bit 0, Channel 2 = bit 1, etc.

def _map_condition(operator: str) -> str:
    """Map alarm operator to Efento condition type."""
    mapping = {
        '>': 'HIGH_THRESHOLD',
        '<': 'LOW_THRESHOLD',
        '>=': 'HIGH_THRESHOLD',
        '<=': 'LOW_THRESHOLD',
        '!=': 'BINARY_CHANGE_STATE'
    }
    return mapping.get(operator, 'HIGH_THRESHOLD')

def _encode_parameters(alarm_data: dict) -> List[int]:
    """Encode alarm thresholds for Efento parameter array."""
    threshold = alarm_data.get('threshold', 0)
    measurement_type = alarm_data.get('measurement_type', 'temperature')

    if measurement_type == 'temperature':
        # Efento expects temperature * 10
        return [int(threshold * 10)]
    elif measurement_type == 'humidity':
        # Humidity as-is
        return [int(threshold)]
    else:
        return [int(threshold)]

def _map_action(level: str) -> str:
    """Map alarm level to Efento action type."""
    mapping = {
        'critical': 'TRIGGER_TRANSMISSION_WITH_ACK',
        'warning': 'TRIGGER_TRANSMISSION',
        'info': 'FAST_ADVERTISING_MODE'
    }
    return mapping.get(level, 'TRIGGER_TRANSMISSION')
```

### Phase 2: Command Consumer Integration

#### 2.1 Generic CommandConsumer

```python
class CommandConsumer:
    def __init__(self, parser_repository, hardware_repository):
        self.parser_repository = parser_repository
        self.hardware_repository = hardware_repository
        self.script_client = ScriptClient()

        async def process_command(self, command_message: dict):
        """Process outgoing command using device-specific parser."""
        device_id = command_message['device_id']

        # Get parser script for this device (same pattern as normalizer)
        parser = await self.parser_repository.get_parser_for_sensor(device_id)
        if not parser:
            raise ValueError(f"No parser found for device {device_id}")

        # Load parser module (same pattern as normalizer)
        script_module = await self.script_client.get_module(parser.file_path)

        # Check if parser supports command formatting
        if not hasattr(script_module, 'format_command'):
            raise ValueError(f"Parser for device {device_id} does not support command formatting")

        # Get translator instance (same pattern as normalizer)
        translator = await self._get_translator_for_device(device_id)

        # Get hardware config (same pattern as normalizer)
        hardware_config = await self._get_hardware_config(device_id)

        # Format command using parser (manufacturer-specific logic)
        binary_command = script_module.format_command(
            command=command_message,
            translator=translator,
            config=hardware_config
        )

        # Store formatted command for protocol-specific delivery
        await self._store_pending_command(device_id, binary_command)
```

#### 2.2 CoAP Integration

```python
# In CoAP resources
class DataRootResource(resource.Resource):
    async def render_GET(self, request):
        device_id = self._extract_device_id(request)

        # Check for pending commands
        pending_commands = await self.command_consumer.get_pending_commands(device_id)

        if pending_commands:
            # Use parser to format command
            command_binary = await self.command_consumer.format_command_for_device(
                device_id, pending_commands[0]
            )

            return aiocoap.Message(payload=command_binary)

        return aiocoap.Message(code=aiocoap.CONTENT)
```

### Phase 3: Configuration Cleanup

#### 3.1 Remove Field Mappings

```yaml
# shared/connectors_config.yaml - BEFORE
connectors:
  coap-connector:
    translators:
      - type: protobuf
        config:
          manufacturer: efento
          device_id_extraction:
            sources:
              - message_type: measurements
                field_path: serial_num
          # REMOVE THESE:
          # outgoing_mapping:
          #   command_type: type
          #   threshold: parameters
          # alarm_mapping:
          #   operator: condition
          #   level: action

# shared/connectors_config.yaml - AFTER
connectors:
  coap-connector:
    translators:
      - type: protobuf
        config:
          manufacturer: efento
          device_id_extraction:
            sources:
              - message_type: measurements
                field_path: serial_num
          # All command formatting logic moved to parser files
```

#### 3.2 Generic Message Parser

```python
class ProtobufMessageParser:
    def parse_dict_to_message(self, data: Dict[str, Any], message_type: str) -> bytes:
        """Generic protobuf encoding - no manufacturer-specific logic."""
        proto_class = self.proto_modules[message_type]['class']
        proto_message = proto_class()

        # Simple field assignment for basic types only
        for field_name, value in data.items():
            if hasattr(proto_message, field_name):
                setattr(proto_message, field_name, value)

        return proto_message.SerializeToString()
```

## Benefits of This Approach

### 1. Single Source of Truth

- All Efento logic in `coap_parser.py`
- All LoRaWAN logic in `lorawan_parser.py`
- Easy to find and modify device-specific behavior

### 2. Consistent Patterns

- Same signature for `parse()` and `format_command()`
- Same initialization process (translator, config, metadata)
- Same error handling patterns

### 3. Simplified Configuration

- No field mappings to maintain
- No manufacturer-specific config sections
- Cleaner, more maintainable YAML files

### 4. Better Testing

- Test entire device behavior in one file
- Mock translator for unit tests
- Integration tests use real parser files

### 5. Easier Onboarding

- New manufacturers only need one parser file
- Clear interface contract to implement
- No configuration complexity
- Generic CommandConsumer works with all manufacturers

## Migration Steps

### Step 1: Enhance Existing Parsers

- Implement `format_command()` in `coap_parser.py`
- Add manufacturer-specific command logic
- Test with sample commands

### Step 2: Create Generic Command Consumer

- Create shared/command_consumer.py (generic, not manufacturer-specific)
- Integrate with parser file loading (same pattern as normalizer)
- Add translator initialization (same pattern as normalizer)
- Test command formatting pipeline with multiple manufacturers

### Step 3: Remove Configuration

- Remove field_mappings from YAML
- Simplify translator classes
- Update documentation

### Step 4: Integration Testing

- Test full command flow
- Verify device responses
- Performance testing

## Example: Complete Efento Parser

```python
# storage/parser_scripts/coap_parser.py

def parse(payload: bytes, translator: Any, metadata: dict, config: dict) -> List[dict]:
    """Handle incoming Efento messages (existing implementation)."""
    # ... existing parse logic

def format_command(command: dict, translator: Any, config: dict) -> bytes:
    """Handle outgoing Efento commands."""
    command_type = command.get('command_type')

    if command_type == "alarm":
        return _format_efento_alarm(command, translator, config)
    elif command_type == "config":
        return _format_efento_config(command, translator, config)
    else:
        raise ValueError(f"Unsupported command type: {command_type}")

def _format_efento_alarm(command: dict, translator: Any, config: dict) -> bytes:
    """Convert alarm to Efento ProtoConfig with rules."""
    alarm_data = command['payload']

    # Get the ProtoConfig class from translator (Efento-specific)
    proto_class = translator.message_parser.proto_modules["config"]["class"]
    proto_message = proto_class()

    # Set basic config fields
    proto_message.measurement_period_base = config.get('measurement_period', 60)
    proto_message.hash = int(time.time())
    proto_message.hash_timestamp = int(time.time())

    # Create Efento-specific rule structure
    rule_msg = proto_message.rules.add()  # Add new rule to repeated field
    rule_msg.channel_mask = 1 << (alarm_data.get('channel', 1) - 1)
    rule_msg.condition = _map_alarm_condition(alarm_data['operator'])
    rule_msg.action = _map_alarm_action(alarm_data['level'])
    rule_msg.enabled = True

    # Set parameters array (Efento-specific encoding)
    threshold = int(alarm_data['threshold'] * 10)  # Temperature * 10
    rule_msg.parameters.append(threshold)

    # Serialize to bytes
    return proto_message.SerializeToString()

# Helper functions for Efento-specific mappings...
```

## Timeline

- **Week 1**: Implement `format_command()` in existing parsers
- **Week 2**: Update CommandConsumer and CoAP integration
- **Week 3**: Remove configuration field mappings
- **Week 4**: Testing and documentation updates

This approach significantly reduces complexity while maintaining all functionality in a more maintainable structure.
