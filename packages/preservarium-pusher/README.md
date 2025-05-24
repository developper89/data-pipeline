# Preservarium Pusher

The Preservarium Pusher service is responsible for pushing commands and settings to IoT devices through Kafka. Commands include parser script references that enable proper command formatting for different device types.

## Overview

The pusher service creates `CommandMessage` objects that contain:

- Device targeting information (device_id, protocol)
- Command data (command_type, payload)
- Parser script reference (stored in the `parser_script_ref` field)

The command consumers (CoAP/MQTT connectors) use the parser script reference to dynamically load the appropriate parser and format commands for specific device types.

## Key Features

- **Dynamic Parser Loading**: Parser scripts are specified per command, eliminating the need for device preregistration
- **Protocol Support**: Supports both MQTT and CoAP protocols
- **Parser Script References**: Commands carry their own parser script reference for flexible device support
- **Caching**: Parser scripts are cached for performance

## Usage Examples

### Basic Command Creation

```python
from preservarium_pusher.service import PusherService
from preservarium_pusher.core.models import CommandMessage

# Initialize the service
pusher = PusherService(
    kafka_bootstrap_servers="localhost:9092",
    command_topic="device_commands"
)
await pusher.initialize()

# Create a command with parser script reference
command = CommandMessage(
    device_id="efento_device_001",
    command_type="config_update",
    payload={
        "measurement_interval": 600,  # 10 minutes
        "request_device_info": True
    },
    protocol="coap",
    parser_script_ref="efento_bidirectional_parser.py"  # Parser script reference
)

# Push the command
success = await pusher.push_command(command)
```

### Using Convenience Methods

```python
# Create command with parser script reference
success = await pusher.create_and_push_command(
    device_id="efento_device_001",
    command_type="time_sync",
    payload={},
    protocol="coap",
    parser_script_ref="efento_bidirectional_parser.py"
)

# Create Efento device command
success = await pusher.create_and_push_command(
    device_id="efento_device_001",
    command_type="config_update",
    payload={
        "measurement_interval": 300,
        "request_device_info": True
    },
    protocol="coap",
    parser_script_ref="efento_bidirectional_parser.py"
)

# Create command for any device type
success = await pusher.create_and_push_command(
    device_id="custom_device_001",
    command_type="reboot",
    payload={},
    protocol="coap",
    parser_script_ref="custom_device_parser.py"
)
```

### MQTT Commands

```python
# MQTT command with topic in metadata
success = await pusher.create_and_push_command(
    device_id="mqtt_device_001",
    command_type="set_led",
    payload={"state": "on", "color": "blue"},
    protocol="mqtt",
    parser_script_ref="mqtt_device_parser.py",
    metadata={"topic": "devices/mqtt_device_001/commands"}
)
```

### Command Dictionary Format

Commands can also be created from dictionaries:

```python
command_dict = {
    "device_id": "efento_device_001",
    "command_type": "config_update",
    "payload": {
        "measurement_interval": 600
    },
    "protocol": "coap",
    "parser_script_ref": "efento_bidirectional_parser.py"
}

success = await pusher.push_command_dict(command_dict)

# MQTT command dictionary
mqtt_command_dict = {
    "device_id": "mqtt_device_001",
    "command_type": "set_led",
    "payload": {"state": "on"},
    "protocol": "mqtt",
    "parser_script_ref": "mqtt_device_parser.py",
    "metadata": {"topic": "devices/mqtt_device_001/commands"}
}

success = await pusher.push_command_dict(mqtt_command_dict)
```

## Parser Script References

The `parser_script_ref` field in `CommandMessage` stores the parser script reference. This allows:

1. **Dynamic Parser Loading**: Each command specifies which parser to use
2. **No Device Preregistration**: No need to preregister devices with specific parsers
3. **Flexible Device Support**: Easy to add new device types by creating new parser scripts

### Example Parser Script Structure

Parser scripts should implement a `format_command` function:

```python
# efento_bidirectional_parser.py
def format_command(command: dict, config: dict) -> bytes:
    """
    Format a command for Efento devices.

    Args:
        command: Command dictionary with type and payload
        config: Device configuration

    Returns:
        Formatted command as bytes
    """
    command_type = command.get('command_type')
    payload = command.get('payload', {})

    if command_type == 'config_update':
        # Create protobuf message for configuration
        # ... implementation specific to Efento devices
        return formatted_bytes
    elif command_type == 'time_sync':
        # Create time sync command
        # ... implementation specific to Efento devices
        return formatted_bytes

    return None  # Unknown command type
```

## How It Works

1. **Command Creation**: Commands are created with parser script references in the `parser_script_ref` field
2. **Kafka Publishing**: Commands are published to Kafka topics for consumption by connectors
3. **Command Processing**: Connectors receive commands and use the `parser_script_ref` field to load the appropriate parser
4. **Dynamic Formatting**: Parser scripts format commands into device-specific binary data
5. **Device Communication**: Formatted commands are sent to devices via MQTT/CoAP

## Migration Notes

**Breaking Change**: The `path` field has been removed. Commands now use the `parser_script_ref` field for parser script references. If you were using the old system, you'll need to:

1. Update command creation to use `parser_script_ref` instead of `path`
2. Update any code that relied on the old `path` field behavior
3. Ensure all commands include the appropriate parser script reference

## Error Handling

The service includes comprehensive error handling:

- Invalid parser script references are caught during command processing
- Missing parsers result in logged errors and command failure
- Network errors during Kafka publishing are handled gracefully
- Malformed commands are validated and rejected

## Configuration

```python
# Environment variables
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DEVICE_COMMAND_TOPIC=device_commands

# Service initialization
pusher = PusherService(
    kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    command_topic=os.getenv("DEVICE_COMMAND_TOPIC")
)
```

## Installation

```bash
# Install from local development directory
uv pip install --system -e /app/packages/preservarium-pusher
```

## Usage

### Pushing Commands to Devices

```python
import asyncio
from preservarium_pusher import create_pusher_service
from preservarium_pusher.core.models import CommandMessage

async def main():
    # Create and initialize the service
    pusher_service = await create_pusher_service(
        kafka_bootstrap_servers="kafka:9092",
        command_topic="device_commands"
    )

    if not pusher_service:
        print("Failed to initialize pusher service")
        return

    try:
        # Create a command for an MQTT device
        command = CommandMessage(
            device_id="device123",
            command_type="set_led",
            payload={"state": "on", "color": "blue"},
            protocol="mqtt",
            parser_script_ref="mqtt_device_parser.py",
            metadata={"topic": "devices/device123/commands"}
        )

        # Push the command
        success = await pusher_service.push_command(command)

        if success:
            print(f"Command sent successfully to device {command.device_id}")
        else:
            print(f"Failed to send command to device {command.device_id}")

    finally:
        # Close the service when done
        await pusher_service.close()

# Run the async function
asyncio.run(main())
```

### Using the Helper Functions

```python
from preservarium_pusher import create_pusher_service

async def control_temperature(device_id, temperature):
    # Initialize the service
    pusher = await create_pusher_service(
        kafka_bootstrap_servers="kafka:9092",
        command_topic="device_commands"
    )

    if not pusher:
        return False

    try:
        # Use the helper function to create and push a command
        return await pusher.create_and_push_command(
            device_id=device_id,
            command_type="set_temperature",
            payload={"temperature": temperature},
            protocol="mqtt",
            parser_script_ref="mqtt_device_parser.py",
            metadata={"topic": f"devices/{device_id}/commands"}
        )
    finally:
        await pusher.close()
```

### Synchronous Usage

```python
from preservarium_pusher import create_pusher_service_sync

# Create the service synchronously
pusher = create_pusher_service_sync(
    kafka_bootstrap_servers="kafka:9092",
    command_topic="device_commands"
)

# Push a command using a dictionary
command_dict = {
    "device_id": "device456",
    "command_type": "reboot",
    "payload": {"delay": 5},
    "protocol": "mqtt",
    "parser_script_ref": "mqtt_device_parser.py",
    "metadata": {"topic": "devices/device456/system"}
}

# Use asyncio to run the async method
import asyncio
loop = asyncio.new_event_loop()
try:
    success = loop.run_until_complete(pusher.push_command_dict(command_dict))
    print(f"Command sent: {success}")
finally:
    loop.run_until_complete(pusher.close())
    loop.close()
```

## Integration with Connectors

This module is designed to work with the existing connector infrastructure. It publishes command messages to a Kafka topic, which is then consumed by the appropriate connectors (MQTT, CoAP) to deliver the commands to the devices.

## License

Proprietary - All rights reserved.

## Protocol-Specific Metadata

The `metadata` field in `CommandMessage` stores protocol-specific data:

### **MQTT Metadata:**

```python
metadata = {
    "topic": "devices/device_001/commands",  # Required for MQTT
    "qos": 1,  # Optional: Quality of Service
    "retain": False  # Optional: Retain flag
}
```

### **CoAP Metadata:**

```python
metadata = {
    "path": "/commands",  # Optional: CoAP resource path
    "confirmable": True  # Optional: Confirmable message
}
```

## Validation

Protocol-specific validation is handled by individual connectors:

- **MQTT Connector**: Validates that `metadata.topic` is present for MQTT commands
- **CoAP Connector**: Handles CoAP-specific options and paths
- **Pusher Service**: Remains protocol-agnostic and doesn't validate protocol-specific requirements
