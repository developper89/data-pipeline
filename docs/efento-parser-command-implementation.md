# Efento Parser-Based Command System Implementation

## Overview

This document provides a detailed step-by-step implementation plan for enhancing the existing `coap_parser.py` to handle outgoing Efento commands using the unified parser-based approach. The plan leverages the existing `CommandConsumer` in `connectors/coap_connector/command_consumer.py` and enhances it to support parser-based command formatting for all commands.

## Current State Analysis

### Existing Components

- ✅ `coap_parser.py` - Handles incoming Efento protobuf messages
- ✅ `ProtobufTranslator` - Generic protobuf encoding/decoding
- ✅ Efento protobuf schemas (`proto_config.proto`, `proto_rule.proto`)
- ✅ Normalizer service infrastructure for parser loading
- ✅ `CommandConsumer` - Existing command consumer in `connectors/coap_connector/command_consumer.py`

### Missing Components

- ❌ `format_command()` function in `coap_parser.py`
- ❌ `parse_dict_to_message()` function in `ProtobufMessageParser`
- ❌ Command formatting configuration in `connectors_config.yaml`
- ❌ Parser-based command formatting in existing command consumer
- ❌ CoAP command response handling

## Implementation Plan

### Phase 1: Enhance CoAP Parser (Week 1)

#### Step 1.0: Add parse_dict_to_message Function to ProtobufMessageParser

**File**: `shared/translation/protobuf/message_parser.py`

```python
def parse_dict_to_message(self, data: Dict[str, Any], message_type: str) -> bytes:
    """
    Convert dictionary data to protobuf message bytes.
    Generic function that works with any protobuf message type.

    Args:
        data: Dictionary containing fields to encode
        message_type: Type of protobuf message to create

    Returns:
        bytes: Serialized protobuf message

    Raises:
        ValueError: If message_type is not available
        RuntimeError: If encoding fails
    """
    if message_type not in self.proto_modules:
        raise ValueError(f"Message type '{message_type}' not available for {self.manufacturer}")

    proto_info = self.proto_modules[message_type]
    proto_class = proto_info['class']

    # Create instance of the protobuf class
    proto_message = proto_class()

    # Generic field assignment - no manufacturer-specific logic
    for field_name, value in data.items():
        if hasattr(proto_message, field_name):
            try:
                setattr(proto_message, field_name, value)
            except Exception as e:
                logger.warning(f"Failed to set field {field_name} = {value}: {e}")
                continue

    # Serialize to bytes
    return proto_message.SerializeToString()
```

#### Step 1.1: Add format_command Function Signature

**File**: `storage/parser_scripts/coap_parser.py`

```python
def format_command(command: dict, translator: Any, config: dict) -> bytes:
    """
    Format commands for Efento devices using protobuf translator.

    Args:
        command: Standardized command dictionary with:
            - command_type: "alarm", "config", "firmware_update", etc.
            - device_id: Target device identifier
            - payload: Command-specific data dictionary
            - metadata: Additional command context
        translator: ProtobufTranslator instance for Efento
        config: Hardware configuration dictionary

    Returns:
        bytes: Protobuf-encoded ProtoConfig message ready for CoAP transmission

    Raises:
        ValueError: If command_type is unsupported
        RuntimeError: If protobuf encoding fails
    """
    logger.info(f"Formatting Efento command: {command.get('command_type')} for device {command.get('device_id')}")

    command_type = command.get('command_type')
    payload = command.get('payload', {})
    device_id = command.get('device_id')

    if command_type == "alarm":
        return _format_efento_alarm_command(payload, translator, config)
    elif command_type == "config":
        return _format_efento_config_command(payload, translator, config)
    elif command_type == "firmware_update":
        return _format_efento_firmware_command(payload, translator, config)
    else:
        raise ValueError(f"Unsupported command type for Efento device: {command_type}")
```

#### Step 1.2: Implement Alarm Command Formatting

```python
def _format_efento_alarm_command(alarm_data: dict, translator: Any, config: dict) -> bytes:
    """
    Convert alarm configuration to Efento ProtoConfig with embedded ProtoRule.

    Args:
        alarm_data: Alarm configuration dict:
            - threshold: numeric threshold value
            - operator: comparison operator (>, <, >=, <=, !=)
            - measurement_type: "temperature", "humidity", etc.
            - channel: channel number (1-based)
            - level: "critical", "warning", "info"
            - enabled: boolean flag
        translator: ProtobufTranslator instance
        config: Hardware configuration

    Returns:
        bytes: Serialized ProtoConfig with embedded rule
    """
    logger.debug(f"Creating Efento alarm rule: {alarm_data}")

    # Validate required alarm fields
    required_fields = ['threshold', 'operator', 'measurement_type', 'channel']
    for field in required_fields:
        if field not in alarm_data:
            raise ValueError(f"Missing required alarm field: {field}")

    # Create Efento-specific rule structure
    efento_rule = {
        'channel_mask': _calculate_efento_channel_mask(alarm_data['channel']),
        'condition': _map_alarm_operator_to_condition(alarm_data['operator']),
        'parameters': _encode_efento_alarm_parameters(alarm_data),
        'action': _map_alarm_level_to_action(alarm_data.get('level', 'warning')),
        'enabled': alarm_data.get('enabled', True)
    }

    # Create ProtoConfig structure with the rule
    proto_config_data = {
        'measurement_period_base': config.get('measurement_period_base', 60),
        'measurement_period_factor': config.get('measurement_period_factor', 1),
        'rules': [efento_rule],  # ProtoConfig.rules is repeated ProtoRule
        'hash': int(time.time()),  # Simple hash for config versioning
        'hash_timestamp': int(time.time())
    }

    # Add additional device configuration if present
    if 'serial_number' in config:
        proto_config_data['serial_number'] = bytes.fromhex(config['serial_number'])

    logger.debug(f"Generated ProtoConfig structure: {proto_config_data}")

    # Create the actual protobuf message structure (Efento-specific logic)
    try:
        # Get the ProtoConfig class from translator
        proto_class = translator.message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()

        # Set basic fields
        proto_message.measurement_period_base = proto_config_data['measurement_period_base']
        proto_message.measurement_period_factor = proto_config_data['measurement_period_factor']
        proto_message.hash = proto_config_data['hash']
        proto_message.hash_timestamp = proto_config_data['hash_timestamp']

        # Handle serial number if present
        if 'serial_number' in proto_config_data:
            proto_message.serial_number = proto_config_data['serial_number']

        # Handle rules array (Efento-specific logic)
        for rule_dict in proto_config_data['rules']:
            rule_msg = proto_message.rules.add()  # Add new rule to repeated field
            rule_msg.channel_mask = rule_dict['channel_mask']
            rule_msg.condition = rule_dict['condition']
            rule_msg.action = rule_dict['action']
            rule_msg.enabled = rule_dict['enabled']

            # Set parameters array
            for param in rule_dict['parameters']:
                rule_msg.parameters.append(param)

        # Serialize to bytes
        binary_config = proto_message.SerializeToString()

        if not binary_config:
            raise RuntimeError("Failed to serialize ProtoConfig")

        logger.info(f"Successfully encoded Efento alarm command: {len(binary_config)} bytes")
        return binary_config

    except Exception as e:
        logger.error(f"Failed to encode Efento alarm command: {e}")
        raise RuntimeError(f"Protobuf encoding failed: {e}") from e
```

#### Step 1.3: Add Efento-Specific Helper Functions

```python
def _calculate_efento_channel_mask(channel: int) -> int:
    """
    Convert 1-based channel number to Efento channel mask.

    Efento uses bit masks where:
    - Channel 1 = bit 0 (mask = 1)
    - Channel 2 = bit 1 (mask = 2)
    - Channel 3 = bit 2 (mask = 4)
    - etc.

    Args:
        channel: 1-based channel number

    Returns:
        int: Channel mask value
    """
    if channel < 1 or channel > 32:
        raise ValueError(f"Channel {channel} out of range (1-32)")

    return 1 << (channel - 1)

def _map_alarm_operator_to_condition(operator: str) -> str:
    """
    Map alarm operator to Efento ProtoRule condition type.

    Args:
        operator: Comparison operator string

    Returns:
        str: Efento condition type
    """
    condition_mapping = {
        '>': 'HIGH_THRESHOLD',
        '>=': 'HIGH_THRESHOLD',
        '<': 'LOW_THRESHOLD',
        '<=': 'LOW_THRESHOLD',
        '==': 'EXACT_VALUE',
        '!=': 'BINARY_CHANGE_STATE',
        'change': 'BINARY_CHANGE_STATE',
        'diff': 'DIFF_THRESHOLD'
    }

    condition = condition_mapping.get(operator)
    if not condition:
        logger.warning(f"Unknown operator '{operator}', defaulting to HIGH_THRESHOLD")
        return 'HIGH_THRESHOLD'

    return condition

def _encode_efento_alarm_parameters(alarm_data: dict) -> List[int]:
    """
    Encode alarm threshold values according to Efento parameter format.

    Efento encoding rules:
    - Temperature: multiply by 10 (25.5°C → 255)
    - Humidity: use as-is (65% → 65)
    - Pressure: use as-is (1013 hPa → 1013)
    - Other: use as-is

    Args:
        alarm_data: Alarm configuration with threshold and measurement_type

    Returns:
        List[int]: Encoded parameter array for ProtoRule
    """
    threshold = alarm_data['threshold']
    measurement_type = alarm_data['measurement_type'].lower()

    if measurement_type in ['temperature', 'temp']:
        # Efento expects temperature * 10
        encoded_value = int(float(threshold) * 10)
    elif measurement_type in ['humidity', 'rh']:
        # Humidity as percentage, no scaling
        encoded_value = int(float(threshold))
    elif measurement_type in ['pressure', 'atmospheric_pressure']:
        # Pressure in hPa, no scaling
        encoded_value = int(float(threshold))
    else:
        # Default: no scaling
        encoded_value = int(float(threshold))
        logger.warning(f"Unknown measurement type '{measurement_type}', using raw value")

    logger.debug(f"Encoded threshold {threshold} ({measurement_type}) → {encoded_value}")

    # Efento parameters array can contain multiple values for complex conditions
    return [encoded_value]

def _map_alarm_level_to_action(level: str) -> str:
    """
    Map alarm severity level to Efento ProtoRule action type.

    Args:
        level: Alarm severity level

    Returns:
        str: Efento action type
    """
    action_mapping = {
        'critical': 'TRIGGER_TRANSMISSION_WITH_ACK',  # Requires acknowledgment
        'high': 'TRIGGER_TRANSMISSION_WITH_ACK',
        'warning': 'TRIGGER_TRANSMISSION',            # Standard transmission
        'medium': 'TRIGGER_TRANSMISSION',
        'info': 'FAST_ADVERTISING_MODE',              # Faster BLE advertising
        'low': 'FAST_ADVERTISING_MODE',
        'debug': 'NO_ACTION'                          # For testing
    }

    action = action_mapping.get(level.lower(), 'TRIGGER_TRANSMISSION')
    logger.debug(f"Mapped alarm level '{level}' → action '{action}'")

    return action
```

#### Step 1.4: Implement Configuration Command Support

```python
def _format_efento_config_command(config_data: dict, translator: Any, config: dict) -> bytes:
    """
    Format device configuration command to Efento ProtoConfig.

    Args:
        config_data: Configuration parameters dict:
            - measurement_period: measurement interval in seconds
            - transmission_interval: how often to send data
            - channels: list of channel configurations
        translator: ProtobufTranslator instance
        config: Hardware configuration

    Returns:
        bytes: Serialized ProtoConfig
    """
    logger.debug(f"Creating Efento config command: {config_data}")

    # Build ProtoConfig structure
    proto_config_data = {
        'hash': int(time.time()),
        'hash_timestamp': int(time.time())
    }

    # Set measurement periods
    if 'measurement_period' in config_data:
        period = config_data['measurement_period']
        # Split into base and factor for Efento format
        base = min(period, 255)  # Max base value
        factor = max(1, period // base)
        proto_config_data['measurement_period_base'] = base
        proto_config_data['measurement_period_factor'] = factor

    # Add other configuration parameters
    config_field_mapping = {
        'transmission_interval': 'transmission_interval',
        'battery_low_threshold': 'battery_low_threshold',
        'encryption_key': 'encryption_key'
    }

    for config_key, proto_field in config_field_mapping.items():
        if config_key in config_data:
            value = config_data[config_key]
            # Handle byte fields
            if config_key == 'encryption_key' and isinstance(value, str):
                value = bytes.fromhex(value)
            proto_config_data[proto_field] = value

    # Create the actual protobuf message structure (Efento-specific logic)
    try:
        # Get the ProtoConfig class from translator
        proto_class = translator.message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()

        # Set all fields from the config data
        for field_name, value in proto_config_data.items():
            if hasattr(proto_message, field_name):
                setattr(proto_message, field_name, value)

        # Serialize to bytes
        binary_config = proto_message.SerializeToString()

        logger.info(f"Successfully encoded Efento config command: {len(binary_config)} bytes")
        return binary_config

    except Exception as e:
        logger.error(f"Failed to encode Efento config command: {e}")
        raise RuntimeError(f"Config encoding failed: {e}") from e

def _format_efento_firmware_command(firmware_data: dict, translator: Any, config: dict) -> bytes:
    """
    Format firmware update command (placeholder for future implementation).

    Args:
        firmware_data: Firmware update parameters
        translator: ProtobufTranslator instance
        config: Hardware configuration

    Returns:
        bytes: Serialized command
    """
    logger.warning("Firmware update commands not yet implemented for Efento devices")
    raise NotImplementedError("Efento firmware updates not implemented")
```

### Phase 2: Enhance Existing Command Consumer (Week 2)

#### Step 2.0: Update Connector Configuration

**File**: `shared/connectors_config.yaml`

Add command formatting configuration to specify which translators and message types to use for different command types:

```yaml
command_formatting:
  sources:
    - command_type: "alarm"
      message_type: "config"
      priority: 1
    - command_type: "config"
      message_type: "config"
      priority: 1
    - command_type: "firmware_update"
      message_type: "config"
      priority: 1
    - command_type: "device_info_request"
      message_type: "device_info"
      priority: 1
  validation:
    supported_commands:
      ["alarm", "config", "firmware_update", "device_info_request"]
```

This configuration allows different command types to use different message formats and translators, providing flexibility for various device manufacturers and command types.

#### Step 2.1: Add Parser-Based Command Formatting to Existing CommandConsumer

**File**: `connectors/coap_connector/command_consumer.py`

The existing `CommandConsumer` already handles Kafka message consumption and command storage. We need to enhance it to support parser-based command formatting for all commands.

```python
# Add these imports at the top of the file
from shared.utils.script_client import ScriptClient
from shared.config_loader import get_translator_configs
from shared.translation.factory import TranslatorFactory
import os

# Enhance the CommandConsumer class with parser-based formatting
class CommandConsumer:
    """
    Enhanced command consumer that uses parser-based command formatting.
    All commands are formatted using device-specific parsers.
    """

    def __init__(self, parser_repository=None, hardware_repository=None):
        """
        Initialize the command consumer with parser support.

        Args:
            parser_repository: Parser repository for parser-based formatting
            hardware_repository: Hardware repository for device configs
        """
        self.consumer = None
        self.running = False
        self._stop_event = asyncio.Event()
        self._task = None

        # In-memory store for pending commands, indexed by device_id
        self.pending_commands: Dict[str, List[Dict[str, Any]]] = {}

        # Parser-based command formatting support (required)
        self.parser_repository = parser_repository
        self.hardware_repository = hardware_repository

        if parser_repository and hardware_repository:
            self.script_client = ScriptClient(
                storage_type=config.SCRIPT_STORAGE_TYPE,
                local_dir=config.LOCAL_SCRIPT_DIR
            )
            # Cache for loaded parsers and translators
            self._parser_cache = {}
            self._translator_cache = {}
            logger.info("Command consumer initialized with parser-based formatting support")
        else:
            logger.warning("Command consumer initialized without parser support - commands will not be processed")

    async def _process_command(self, command_data: Dict[str, Any]):
        """
        Process command using parser-based formatting.

        Args:
            command_data: The command data from Kafka
        """
        try:
            # Check if this is a CoAP command
            if command_data.get('protocol', '').lower() != 'coap':
                logger.debug(f"Ignoring non-CoAP command: {command_data.get('protocol')}")
                return

            # Extract required fields
            device_id = command_data.get('device_id')
            request_id = command_data.get('request_id', 'unknown')

            if not device_id:
                logger.error(f"[{request_id}] Invalid command message: missing device_id")
                return

            # Format command using parser-based approach
            if self.parser_repository and self.hardware_repository:
                formatted_command = await self._format_command_with_parser(device_id, command_data)
                if formatted_command:
                    # Store the formatted command
                    await self._store_formatted_command(device_id, command_data, formatted_command)
                    logger.info(f"[{request_id}] Formatted and stored command for device {device_id}")
                    return

            # If we get here, we couldn't format the command
            logger.error(f"[{request_id}] Unable to format command for device {device_id}: no parser available")

        except Exception as e:
            logger.exception(f"Error processing command: {e}")

    async def _store_formatted_command(self, device_id: str, command_data: Dict[str, Any], formatted_command: bytes):
        """Store a command formatted by parser."""
        # Convert formatted binary command to hex string for storage
        hex_command = formatted_command.hex()

        # Update command data with formatted payload
        enhanced_command = command_data.copy()
        enhanced_command['payload'] = {
            'formatted_command': hex_command,
            'formatted_by': 'parser',
            'original_payload': command_data.get('payload', {})
        }
        enhanced_command['stored_at'] = time.time()

        # Store the enhanced command
        if device_id not in self.pending_commands:
            self.pending_commands[device_id] = []
        self.pending_commands[device_id].append(enhanced_command)

    async def _format_command_with_parser(self, device_id: str, command_data: dict) -> Optional[bytes]:
        """
        Format command using device-specific parser (same pattern as normalizer).

        Args:
            device_id: Target device identifier
            command_data: Command data dictionary

        Returns:
            bytes: Formatted binary command or None if formatting fails
        """
        try:
            # Get parser script for device (same as normalizer)
            parser = await self.parser_repository.get_parser_for_sensor(device_id)
            if not parser:
                logger.error(f"No parser found for device {device_id}")
                return None

            # Load parser module (with caching)
            script_module = await self._get_parser_module(parser.file_path)
            if not script_module:
                logger.error(f"Failed to load parser for device {device_id}")
                return None

            # Check if parser supports command formatting
            if not hasattr(script_module, 'format_command'):
                logger.error(f"Parser for device {device_id} does not support command formatting")
                return None

            # Get translator instance based on command type
            command_type = command_data.get('command_type')
            translator = await self._get_translator_for_device(device_id, command_type)

            # Get hardware configuration (same as normalizer)
            hardware_config = await self._get_hardware_config(device_id)

            # Format command using parser
            binary_command = script_module.format_command(
                command=command_data,
                translator=translator,
                config=hardware_config
            )

            return binary_command

        except Exception as e:
            logger.exception(f"Error formatting command for device {device_id}: {e}")
            return None

    async def _get_parser_module(self, file_path: str):
        """Get parser module with caching."""
        if file_path not in self._parser_cache:
            try:
                filename = os.path.basename(file_path)
                script_ref = os.path.join(self.script_client.local_dir, filename)
                module = await self.script_client.get_module(script_ref)
                self._parser_cache[file_path] = module
            except Exception as e:
                logger.error(f"Failed to load parser module {file_path}: {e}")
                return None

        return self._parser_cache[file_path]

    async def _get_translator_for_device(self, device_id: str, command_type: str = None):
        """Get translator instance for device based on command type."""
        cache_key = f"{command_type or 'default'}_translator"

        if cache_key not in self._translator_cache:
            try:
                # Get translator configs for coap-connector
                translator_configs = get_translator_configs("coap-connector")

                # Find appropriate translator based on command type
                selected_translator_config = None

                for translator_config in translator_configs:
                    # Check if this translator supports the command type
                    command_formatting = translator_config.get('config', {}).get('command_formatting', {})
                    if command_formatting:
                        supported_commands = command_formatting.get('validation', {}).get('supported_commands', [])
                        if command_type in supported_commands:
                            selected_translator_config = translator_config
                            break

                # Fallback to first available translator if no specific match
                if not selected_translator_config and translator_configs:
                    selected_translator_config = translator_configs[0]

                if selected_translator_config:
                    translator = TranslatorFactory.create_translator(selected_translator_config)
                    self._translator_cache[cache_key] = translator
                else:
                    logger.error("No suitable translator configuration found")
                    return None

            except Exception as e:
                logger.error(f"Failed to create translator: {e}")
                return None

        return self._translator_cache[cache_key]

    async def _get_hardware_config(self, device_id: str) -> Dict[str, Any]:
        """Get hardware configuration for device."""
        try:
            hardware = await self.hardware_repository.get_hardware_by_sensor_parameter(device_id)
            return hardware.configuration if hardware else {}
        except Exception as e:
            logger.error(f"Error fetching hardware config for {device_id}: {e}")
            return {}
```

#### Step 2.2: Integrate Enhanced Command Consumer with CoAP Server

**File**: `connectors/coap_connector/resources.py`

The existing `DataRootResource` already has basic command handling via `get_formatted_command()`. We need to enhance it to work with the improved command consumer.

```python
# Update DataRootResource to use enhanced command consumer

class DataRootResource(resource.Resource):
    def __init__(self, translation_manager, command_consumer=None):
        super().__init__()
        self.translation_manager = translation_manager
        self.command_consumer = command_consumer  # Enhanced command consumer

    async def render_GET(self, request):
        """Handle CoAP GET requests - check for pending commands."""
        try:
            # Extract device ID (existing logic)
            device_id = await self._extract_device_id(request)

            if not device_id:
                logger.warning("No device ID found in CoAP request")
                return aiocoap.Message(code=aiocoap.BAD_REQUEST)

            # Check for pending commands first (existing method works with enhancements)
            if self.command_consumer:
                pending_command = await self.command_consumer.get_formatted_command(device_id)
                if pending_command:
                    logger.info(f"Sending command to device {device_id}: {len(pending_command)} bytes")
                    return aiocoap.Message(
                        code=aiocoap.CONTENT,
                        payload=pending_command
                    )

            # No commands pending - return normal response
            logger.debug(f"No pending commands for device {device_id}")
            return aiocoap.Message(code=aiocoap.CONTENT)

        except Exception as e:
            logger.exception(f"Error handling CoAP GET request: {e}")
            return aiocoap.Message(code=aiocoap.INTERNAL_SERVER_ERROR)
```

**Note**: The existing `get_formatted_command()` method in the current `CommandConsumer` will work seamlessly with the parser-based formatting enhancements, reading the formatted hex command from the stored payload.

### Phase 4: Deployment and Monitoring (Week 4)

#### Step 4.1: Update CoAP Server Main

**File**: `connectors/coap_connector/main.py`

```python
# Enhance existing command consumer initialization

async def main():
    """Main entry point for CoAP connector with enhanced command support."""
    # ... existing initialization ...

    # Import the existing command consumer (already imported)
    from command_consumer import CommandConsumer

    # Initialize command consumer with parser support
    # The existing command consumer now accepts optional repositories
    command_consumer = CommandConsumer(
        parser_repository=parser_repository,
        hardware_repository=hardware_repository
    )

    # Initialize resources with enhanced command consumer
    root_resource = DataRootResource(
        translation_manager=translation_manager,
        command_consumer=command_consumer
    )

    # Start command consumer in background (existing pattern)
    asyncio.create_task(command_consumer.start())

    # ... existing server setup ...
```

**Note**: The existing `main.py` already imports and initializes the `CommandConsumer`. We need to pass the repositories to enable parser-based formatting for all commands.

## Success Criteria

✅ `coap_parser.py` successfully formats device-specific commands (Efento example)
✅ Command formatting configuration supports multiple translators and message types
✅ Enhanced existing command consumer processes all commands using parser-based formatting
✅ CoAP server delivers binary commands to devices using existing infrastructure
✅ Real devices accept and execute commands
✅ End-to-end latency < 5 seconds for alarm commands
✅ Zero configuration changes needed for new command types
✅ Parser-based approach works for multiple manufacturers
✅ Adding new manufacturers requires only parser file creation
✅ Seamless integration with existing command consumer infrastructure

This implementation provides a complete, testable system for device command formatting using the unified parser approach that works with any manufacturer while leveraging the existing command consumer infrastructure.
