# CoAP Connector - Preservarium SDK Integration Plan

## 1. Overview

This plan details how to integrate the Preservarium SDK with the CoAP connector service to leverage the `SQLParserRepository` for dynamically loading device parsers, similar to the implementation in the normalizer service.

## 2. Files to Modify

1. `connectors/coap_connector/Dockerfile.dev`
2. `connectors/coap_connector/command_consumer.py`
3. `connectors/coap_connector/coap_server.py` (assuming this is the startup file)
4. `connectors/coap_connector/resources.py`

## 3. Detailed Implementation Changes

### 3.1. Dockerfile.dev Updates

```dockerfile
# Base image and initial setup remains the same

# Install preservarium-sdk
COPY ./packages/preservarium-sdk /app/packages/preservarium-sdk
RUN pip install -e /app/packages/preservarium-sdk

# Install any additional dependencies required by the SDK
RUN pip install sqlalchemy psycopg2-binary

# Rest of Dockerfile remains the same
```

### 3.2. command_consumer.py Updates

```python
# Add imports
from preservarium_sdk.repositories.parser_repository import SQLParserRepository
import importlib.util
import sys
import os

class CommandConsumer:
    def __init__(self, kafka_config, topics, **kwargs):
        # Existing initialization code
        self.kafka_consumer = KafkaConsumer(...)
        self.pending_commands = {}

        # Add SQLParserRepository initialization
        self.parser_repository = SQLParserRepository()

        # Initialize parser registry - maps device_id to parser module
        # Parsers are now loaded dynamically when commands are processed
        self.device_parser_map = {}

    def _load_parser_module(self, parser_path):
        """
        Dynamically load a parser module from its file path.

        Args:
            parser_path (str): Path to the parser module

        Returns:
            module: Loaded parser module or None if loading fails
        """
        try:
            # Extract module name from path
            module_name = os.path.splitext(os.path.basename(parser_path))[0]

            # Load module spec
            spec = importlib.util.spec_from_file_location(module_name, parser_path)
            if not spec:
                logger.error(f"Could not load spec for module: {parser_path}")
                return None

            # Load module
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Add to sys.modules
            sys.modules[module_name] = module

            logger.debug(f"Successfully loaded parser module: {module_name}")
            return module
        except Exception as e:
            logger.exception(f"Error loading parser module from {parser_path}: {e}")
            return None

    def register_device(self, device_id, device_type):
        """
        Register a specific device ID with its device type to use the appropriate parser.
        Note: This method is now called on-demand when commands are processed.

        Args:
            device_id (str): The unique ID of the device
            device_type (str): The type of the device (e.g., 'efento')
        """
        try:
            # Check if we already have a parser for this device type
            if device_type in self.device_parser_map:
                parser_module = self.device_parser_map[device_type]
                self.device_parser_map[device_id] = parser_module
                logger.info(f"Registered device {device_id} with existing parser for type {device_type}")
                return True

            # Try to load parser for this device type if not already loaded
            parser_info = self.parser_repository.get_parser_for_sensor(device_type)
            if parser_info and 'parser_path' in parser_info:
                parser_module = self._load_parser_module(parser_info['parser_path'])
                if parser_module:
                    # Store for both device type and specific device ID
                    self.device_parser_map[device_type] = parser_module
                    self.device_parser_map[device_id] = parser_module
                    logger.info(f"Registered device {device_id} with newly loaded parser for type {device_type}")
                    return True

            logger.warning(f"Could not register device {device_id} - no parser found for type {device_type}")
            return False

        except Exception as e:
            logger.exception(f"Error registering device {device_id} of type {device_type}: {e}")
            return False

    async def get_formatted_command(self, device_id):
        """
        Get a command formatted for the specific device.
        Now loads parser scripts dynamically from command parser_script_ref.

        Args:
            device_id (str): Device ID to get commands for

        Returns:
            bytes: Formatted command payload or None if no commands or parser found
        """
        pending_commands = self.get_pending_commands(device_id)
        if not pending_commands:
            return None

        command = pending_commands[0]  # Get first pending command

        # Get parser script reference from command
        parser_script_ref = command.get('parser_script_ref')
        if not parser_script_ref:
            logger.warning(f"No parser_script_ref found in command for device {device_id}")
            return None

        # Load parser module dynamically if not already cached
        parser_module = self.device_parser_map.get(device_id)
        if not parser_module:
            parser_module = self._load_parser_module(parser_script_ref)
            if parser_module:
                self.device_parser_map[device_id] = parser_module
            else:
                logger.error(f"Failed to load parser script {parser_script_ref} for device {device_id}")
                return None

        try:
            # Check if the parser has the format_command function
            if hasattr(parser_module, 'format_command'):
                # Format the command - this assumes the interface matches what we need
                formatted_command = parser_module.format_command(command)

                if isinstance(formatted_command, bytes):
                    logger.info(f"Successfully formatted command for device {device_id}")
                    return formatted_command
                else:
                    logger.error(f"Parser for {device_id} returned non-bytes data: {type(formatted_command)}")
                    return None
            else:
                logger.warning(f"Parser for device {device_id} does not have format_command function")
                return None

        except Exception as e:
            logger.exception(f"Error formatting command for device {device_id}: {e}")
            return None

    # Rest of the CommandConsumer class methods remain the same
```

### 3.3. coap_server.py Updates (or main startup file)

```python
# Add imports
from preservarium_sdk.repositories.parser_repository import SQLParserRepository

def main():
    # Existing initialization code

    # Initialize Kafka producer
    kafka_producer = KafkaMsgProducer(kafka_config)

    # Initialize command consumer with SQLParserRepository
    command_consumer = CommandConsumer(kafka_config, command_topics)

    # Initialize CoAP server
    server = CoapServer(
        host=config.COAP_HOST,
        port=config.COAP_PORT,
        kafka_producer=kafka_producer,
        command_consumer=command_consumer
    )

    # Start server
    asyncio.run(server.start())

if __name__ == "__main__":
    main()
```

### 3.4. resources.py Updates

```python
# No significant changes needed to imports
# The changes are minimal as most of the logic is in the CommandConsumer class

class DeviceDataHandlerResource(resource.Resource):
    # Most of the class remains the same

    async def _process_request(self, request: aiocoap.Message, method: str) -> aiocoap.Message:
        """
        Common logic for processing POST/PUT.
        Also includes pending commands in the response if any exist.
        """
        # Existing logic remains the same

        try:
            # Process the message and publish to Kafka
            # ...existing code...

            # Check for pending commands for this device
            # The command_consumer.get_formatted_command now uses the SDK-loaded parsers
            formatted_command = await self.command_consumer.get_formatted_command(self.device_id)

            if formatted_command:
                # Get the command details for logging
                pending_commands = self.command_consumer.get_pending_commands(self.device_id)
                command_id = pending_commands[0].get("request_id", "unknown") if pending_commands else "unknown"

                logger.info(f"[{request_id}] Sending formatted command {command_id} to device {self.device_id}")

                # Acknowledge the command was sent (removes from pending)
                if pending_commands:
                    self.command_consumer.acknowledge_command(self.device_id, command_id)

                # Send the formatted binary command in response
                success_code = aiocoap.Code.CHANGED if method == "PUT" else aiocoap.Code.CREATED
                return aiocoap.Message(code=success_code, payload=formatted_command)
            else:
                # No commands pending or no parser available - just send success code
                success_code = aiocoap.Code.CHANGED if method == "PUT" else aiocoap.Code.CREATED
                logger.debug(f"[{request_id}] Successfully processed data for {self.device_id}. No commands sent.")
                return aiocoap.Message(code=success_code)

        except Exception as e:
            # Error handling code remains the same
            # ...
```

## 4. Additional Requirements

### 4.1. Device Registration Process

To fully implement this integration, we need a way to map device IDs to device types. Options include:

1. Add a device registry in the database accessed via the SDK
2. Extract device type from incoming messages if available
3. Maintain a configuration file that maps device IDs to device types
4. Add an API endpoint to register devices with their types

### 4.2. Error Handling and Logging

The implementation includes comprehensive error handling and logging to help with troubleshooting:

- Logs when parsers are registered
- Logs when commands are formatted
- Logs errors when parser loading fails
- Logs when commands cannot be formatted due to missing parsers

### 4.3. Testing Strategy

To verify the implementation:

1. Test loading parsers from the SDK repository
2. Test registering devices with different types
3. Test formatting commands for different device types
4. Test the full flow from command creation to delivery

## 5. Implementation Steps

1. Update the Dockerfile.dev first to ensure the SDK is available
2. Implement the CommandConsumer changes
3. Update the startup file
4. Test with a known device type
5. Add logging and verification
