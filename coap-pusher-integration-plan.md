# CoAP Command Pushing Integration Plan

## Problem Statement

Our current architecture faces a challenge with the pusher module:

1. We've separated device-specific parsing logic into dedicated parser scripts (e.g., `efento_parser.py`)
2. The pusher module is device-agnostic and doesn't understand device-specific protocols
3. Commands need to be encoded in device-specific formats (e.g., Efento requires protobuf-encoded messages)
4. The CoAP connector returns commands in responses to device requests
5. We need to support multiple device types with different command formats

## Proposed Solution: Bidirectional Parser Architecture

We'll extend the parser script architecture to support bidirectional transformation - not just from device format to standardized format, but also from standardized command format to device-specific format.

## Key Components

### 1. Enhanced Parser Scripts

Extend parser scripts to include two main functions:

- `parse()`: (existing) Converts device-specific data to standardized format
- `format_command()`: (new) Converts standardized commands to device-specific format

```python
# Example updated parser script structure
def parse(payload: bytes, config: dict) -> List[dict]:
    """Convert device data to standardized format (existing)"""
    # Existing parsing logic...
    return standardized_outputs

def format_command(command: dict, config: dict) -> bytes:
    """Convert standardized command to device-specific format (new)"""
    # Device-specific command encoding logic
    # For Efento: Create and encode protobuf messages
    return encoded_command_bytes
```

### 2. Command Registry Service

A component that maps device IDs to their appropriate parser scripts:

```python
class CommandRegistry:
    def __init__(self):
        self.parsers = {}  # device_id -> parser_module

    async def register_device(self, device_id: str, parser_script_ref: str):
        """Register a device with its parser script"""
        # Load the parser module dynamically
        parser_module = await self._load_script(parser_script_ref)
        self.parsers[device_id] = parser_module

    async def format_command(self, device_id: str, command: dict, config: dict) -> Optional[bytes]:
        """Format a command for a specific device using its parser"""
        parser = self.parsers.get(device_id)
        if not parser or not hasattr(parser, 'format_command'):
            return None
        return parser.format_command(command, config)
```

### 3. Enhanced Command Consumer

Update the command consumer to use the registry:

```python
class CommandConsumer:
    def __init__(self, command_registry: CommandRegistry):
        self.command_registry = command_registry
        self.pending_commands = {}  # device_id -> [commands]

    async def _process_command(self, command_data: dict):
        device_id = command_data.get('device_id')
        # Get device configuration from database or cache
        device_config = await self._get_device_config(device_id)

        # Store the raw command data
        if device_id not in self.pending_commands:
            self.pending_commands[device_id] = []
        self.pending_commands[device_id].append(command_data)

    async def get_formatted_command(self, device_id: str) -> Optional[bytes]:
        """Get a formatted command for a device"""
        if not self.pending_commands.get(device_id):
            return None

        command = self.pending_commands[device_id][0]
        device_config = await self._get_device_config(device_id)

        # Format command using registry
        formatted_command = await self.command_registry.format_command(
            device_id, command, device_config
        )

        return formatted_command
```

### 4. Updated CoAP Resource Handler

```python
class DeviceDataHandlerResource(resource.Resource):
    # ...existing code...

    async def _process_request(self, request, method):
        # ...existing processing code...

        # After processing the incoming data, check for pending commands
        formatted_command = await self.command_consumer.get_formatted_command(self.device_id)

        if formatted_command:
            # Send the formatted command as the response payload
            command_id = pending_commands[0].get("request_id", "unknown")
            logger.info(f"Sending formatted command {command_id} to device {self.device_id}")

            # Acknowledge the command was sent
            self.command_consumer.acknowledge_command(self.device_id, command_id)

            # Return with the device-specific formatted payload
            return aiocoap.Message(code=success_code, payload=formatted_command)
        else:
            # No commands pending, send normal response
            return aiocoap.Message(code=success_code)
```

### 5. Device Config Management

Integrate with device management to retrieve parser script references and configurations:

```python
async def _get_device_config(device_id: str) -> dict:
    """Get device configuration from database"""
    # Query database for device configuration, including:
    # - parser_script_ref: Path to the parser script
    # - device-specific configuration
    return config
```

## Device Registration Process

The `register_device` method is a critical part of the system that connects devices to their parser scripts. Here's how and when it will be called:

### 1. Automatic Registration on First Data Receipt

```python
class DeviceDataHandlerResource(resource.Resource):
    async def _process_request(self, request, method):
        # ... existing code ...

        # After processing the incoming data
        device_id = self.device_id

        # Check if device needs registration
        if not self.command_consumer.command_registry.is_registered(device_id):
            # Query the device config from database
            device_config = await self._get_device_config(device_id)
            if device_config and 'parser_script_ref' in device_config:
                await self.command_consumer.command_registry.register_device(
                    device_id, device_config['parser_script_ref']
                )
                logger.info(f"Registered device {device_id} with parser {device_config['parser_script_ref']}")
```

### 2. Bulk Registration on Service Startup

```python
class CoapGatewayServer:
    async def start(self):
        # ... existing initialization code ...

        # Initialize the command registry
        command_registry = CommandRegistry()

        # Register all known devices from the database
        devices = await self._get_all_known_devices()
        for device in devices:
            if 'device_id' in device and 'parser_script_ref' in device:
                await command_registry.register_device(
                    device['device_id'], device['parser_script_ref']
                )
                logger.info(f"Pre-registered device {device['device_id']} with parser {device['parser_script_ref']}")

        # Initialize command consumer with the registry
        self.command_consumer = CommandConsumer(command_registry)
        # ... rest of startup code ...
```

### 3. Registration API for Device Management

Add an endpoint to the management API for registering new devices:

```python
# In the management API service
class DeviceAPI:
    def __init__(self, command_registry: CommandRegistry):
        self.command_registry = command_registry

    async def register_device(self, device_id: str, parser_script_ref: str, config: dict):
        """API endpoint to register a device"""
        # Store device config in the database
        await self._store_device_config(device_id, config)

        # Register with the command registry
        await self.command_registry.register_device(device_id, parser_script_ref)

        return {"success": True, "message": f"Device {device_id} registered successfully"}
```

### 4. Lazy Registration

Alternatively, the registry could register devices lazily when commands need to be formatted:

```python
class CommandRegistry:
    async def format_command(self, device_id: str, command: dict, config: dict) -> Optional[bytes]:
        """Format a command for a specific device, registering parser if needed"""
        parser = self.parsers.get(device_id)

        # If parser not found, try to register it
        if not parser and 'parser_script_ref' in config:
            try:
                await self.register_device(device_id, config['parser_script_ref'])
                parser = self.parsers.get(device_id)
            except Exception as e:
                logger.error(f"Failed to register device {device_id}: {e}")
                return None

        if not parser or not hasattr(parser, 'format_command'):
            return None

        return parser.format_command(command, config)
```

### 5. Cache Expiration and Refreshing

```python
class CommandRegistry:
    def __init__(self, cache_ttl: int = 3600):  # Default 1 hour TTL
        self.parsers = {}  # device_id -> (parser_module, timestamp)
        self.cache_ttl = cache_ttl

    async def format_command(self, device_id: str, command: dict, config: dict) -> Optional[bytes]:
        # Check if parser is in cache and not expired
        if device_id in self.parsers:
            parser, timestamp = self.parsers[device_id]
            if time.time() - timestamp > self.cache_ttl:
                # Refresh the parser
                try:
                    parser = await self._load_script(config['parser_script_ref'])
                    self.parsers[device_id] = (parser, time.time())
                except Exception as e:
                    logger.error(f"Failed to refresh parser for device {device_id}: {e}")

        # ... rest of the method ...
```

## Example: Efento Device Command Implementation

For Efento devices, the `format_command` function in the parser script would:

```python
def format_command(command: dict, config: dict) -> bytes:
    """Format a command for an Efento device"""
    # Extract command parameters
    command_type = command.get('command_type')
    payload = command.get('payload', {})

    if command_type == 'config_update':
        # Create a configuration protobuf message
        proto_config = proto_config_pb2.ProtoConfig()

        # Set basic configuration from payload
        proto_config.request_device_info = payload.get('request_device_info', True)
        proto_config.current_time = int(time.time())

        if 'measurement_interval' in payload:
            proto_config.measurement_interval_minutes = payload['measurement_interval'] // 60

        # Add other configuration parameters
        # ... (similar to the code in the EfentoConfigurator.create_config_message method)

        # Serialize and return
        return proto_config.SerializeToString()

    elif command_type == 'time_sync':
        # Return current time in the format Efento expects
        time_stamp = int(time.time())
        time_stamp_hex = hex(time_stamp)
        return bytearray.fromhex(time_stamp_hex[2:])

    # Add other command types as needed

    return None  # Unknown command type
```

## Implementation Steps

1. **Update Parser Framework**

   - Create a base class or interface for bidirectional parsers
   - Update documentation for parser script development
   - Implement example parsers for key device types (starting with Efento)

2. **Build Command Registry**

   - Implement the command registry service
   - Add caching for better performance
   - Create interfaces for registering and retrieving parsers

3. **Enhance Command Consumer**

   - Update the command consumer to use the registry
   - Add metrics and telemetry for command handling

4. **Update CoAP Resource Handler**

   - Modify to work with the new command formatting system
   - Add proper error handling for formatting failures

5. **Database Integration**

   - Ensure device configurations include parser script references
   - Add caching for frequently accessed configurations

6. **Testing**
   - Unit tests for each component
   - Integration tests for the full command flow
   - Device-specific tests for each supported device type

## Benefits

1. **Extensibility**: Easy to add support for new device types
2. **Separation of Concerns**: Device-specific logic stays in parser scripts
3. **Reusability**: Parser scripts handle both directions of communication
4. **Performance**: Registry and caching improve command formatting speed
5. **Maintainability**: Modular approach simplifies updates to device support

## Metrics and Monitoring

We'll add metrics for:

- Command formatting success/failure rates
- Command delivery times
- Parser script loading times
- Registry cache hit/miss rates
