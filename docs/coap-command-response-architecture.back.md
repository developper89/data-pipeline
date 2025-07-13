# CoAP Command Response Architecture Plan

## Problem Statement

The CoAP connector's command consumer needs to generate appropriate responses when delivering commands to devices. However, response generation logic is manufacturer-specific:

- **Efento devices**: Require `current_time` in response payload
- **Other manufacturers**: May require different response formats
- **Generic devices**: May only need standard CoAP ACK

The challenge is maintaining device-agnostic code in the command consumer while supporting manufacturer-specific response logic that belongs in parser scripts.

## Current Architecture Limitation

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CommandConsumer   │    │    DataRootResource │    │   CoAP Device   │
│   (Device Agnostic) │    │   (Protocol Handler)│    │  (Manufacturer  │
│                     │    │                     │    │   Specific)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                        │                        │
         │ 1. Format Command      │                        │
         │                        │                        │
         │ 2. Store Command       │                        │
         │                        │                        │
         │                        │ 3. Device Contacts     │
         │                        │◄──────────────────────│
         │                        │                        │
         │ 4. Get Command         │                        │
         │◄──────────────────────│                        │
         │                        │                        │
         │ 5. Return Command      │                        │
         │──────────────────────▶│                        │
         │                        │                        │
         │ ❌ MISSING: Response   │ 6. Send Command +      │
         │    Generation Logic    │    Response            │
         │                        │──────────────────────▶│
```

**Issue**: Step 6 requires manufacturer-specific response generation, but this logic cannot be in the device-agnostic `CommandConsumer` or `DataRootResource`.

## Proposed Solution: Parser-Based Response Generation

### Architecture Overview

Extend the parser interface to include response generation alongside command formatting:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CommandConsumer   │    │    DataRootResource │    │   CoAP Device   │
│   (Device Agnostic) │    │   (Protocol Handler)│    │  (Manufacturer  │
│                     │    │                     │    │   Specific)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                        │                        │
         │ 1. Format Command      │                        │
         │    + Response          │                        │
         │                        │                        │
         │ 2. Store Command       │                        │
         │    + Response          │                        │
         │                        │                        │
         │                        │ 3. Device Contacts     │
         │                        │◄──────────────────────│
         │                        │                        │
         │ 4. Get Command +       │                        │
         │    Response            │                        │
         │◄──────────────────────│                        │
         │                        │                        │
         │ 5. Return Command +    │                        │
         │    Response            │                        │
         │──────────────────────▶│                        │
         │                        │                        │
         │                        │ 6. Send Command +      │
         │                        │    Response            │
         │                        │──────────────────────▶│
```

### Core Components

#### 1. Extended Parser Interface

Add a new mandatory function to all parser scripts:

```python
def format_response(command: dict, translator: Any, config: dict) -> bytes:
    """
    Generate appropriate response payload for command delivery.

    Args:
        command: The command that was sent to the device
        translator: Translator instance for device communication
        config: Hardware configuration dictionary

    Returns:
        bytes: Response payload to include in CoAP response

    Raises:
        NotImplementedError: If response generation not supported
        ValueError: If command format is invalid
    """
```

#### 2. Enhanced Command Storage

Modify the command storage structure to include response data:

```python
@dataclass
class FormattedCommand:
    """Enhanced command storage with response generation."""
    command_bytes: bytes
    response_bytes: bytes
    command_metadata: Dict[str, Any]
    response_metadata: Dict[str, Any]
    created_at: float
    expires_at: Optional[float] = None
```

#### 3. Modified Command Consumer Flow

Update the command processing flow to generate both command and response:

```python
async def _format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Optional[FormattedCommand]:
    """
    Format both command and response using device-specific parser.

    Returns:
        FormattedCommand with both command and response bytes
    """
    # ... existing command formatting logic ...

    # Generate response using parser
    response_bytes = script_module.format_response(
        command=command_dict,
        translator=translator,
        config=hardware_config
    )

    return FormattedCommand(
        command_bytes=command_bytes,
        response_bytes=response_bytes,
        command_metadata={"formatted_by": "parser"},
        response_metadata={"generated_at": time.time()},
        created_at=time.time()
    )
```

## Implementation Details

### 1. Parser Script Changes

#### Efento Parser (`coap_parser.py`)

```python
def format_response(command: dict, translator: Any, config: dict) -> bytes:
    """
    Generate Efento-specific response payload.

    For Efento devices, response should contain current time configuration.
    """
    logger.info(f"Generating Efento response for command: {command.get('command_type')}")

    try:
        # Create ProtoConfig with current time
        proto_class = translator.message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()

        # Set current time (Efento-specific requirement)
        proto_message.current_time = int(time.time())
        proto_message.request_configuration = True

        # Serialize to bytes
        response_bytes = proto_message.SerializeToString()

        if not response_bytes:
            raise RuntimeError("Failed to serialize response")

        logger.info(f"Generated Efento response: {len(response_bytes)} bytes")
        return response_bytes

    except Exception as e:
        logger.error(f"Failed to generate Efento response: {e}")
        raise RuntimeError(f"Response generation failed: {e}") from e
```

#### Generic Parser Template

```python
def format_response(command: dict, translator: Any, config: dict) -> bytes:
    """
    Generate generic response payload.

    For devices that don't require specific response data,
    return empty payload (standard CoAP ACK).
    """
    logger.info(f"Generating generic response for command: {command.get('command_type')}")

    # Most devices don't need specific response payload
    # CoAP ACK is sufficient
    return b""
```

### 2. Command Consumer Changes

#### Enhanced Command Storage

```python
class CommandConsumer:
    def __init__(self):
        # Change from simple dict to structured storage
        self.pending_commands: Dict[str, List[FormattedCommand]] = {}

    async def _store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_command: FormattedCommand):
        """Store formatted command with response."""
        enhanced_command = {
            'request_id': command_message.request_id,
            'command_type': command_message.command_type,
            'device_id': device_id,
            'command_payload': formatted_command.command_bytes.hex(),
            'response_payload': formatted_command.response_bytes.hex(),
            'formatted_by': 'parser',
            'created_at': formatted_command.created_at,
            'expires_at': formatted_command.expires_at
        }

        if device_id not in self.pending_commands:
            self.pending_commands[device_id] = []
        self.pending_commands[device_id].append(enhanced_command)
```

#### Enhanced Command Retrieval

```python
async def get_formatted_command_with_response(self, device_id: str) -> Optional[Tuple[bytes, bytes]]:
    """
    Get formatted command and response for a device.

    Returns:
        Tuple of (command_bytes, response_bytes) or None if no commands available
    """
    if not self.pending_commands.get(device_id):
        return None

    command = self.pending_commands[device_id][0]

    try:
        command_hex = command.get('command_payload', '')
        response_hex = command.get('response_payload', '')

        command_bytes = bytes.fromhex(command_hex) if command_hex else b""
        response_bytes = bytes.fromhex(response_hex) if response_hex else b""

        return command_bytes, response_bytes

    except Exception as e:
        logger.exception(f"Error getting formatted command and response: {e}")
        return None
```

### 3. DataRootResource Changes

#### Enhanced Response Generation

```python
async def render(self, request):
    """Enhanced request handler with response generation."""
    # ... existing code for device ID extraction and raw message creation ...

    # Check for pending commands
    if self.command_consumer:
        try:
            command_response = await self.command_consumer.get_formatted_command_with_response(translation_result.device_id)

            if command_response:
                command_bytes, response_bytes = command_response

                # Get command details for logging
                pending_commands = self.command_consumer.get_pending_commands(translation_result.device_id)
                command_id = pending_commands[0].get("request_id", "unknown") if pending_commands else "unknown"

                logger.info(f"[{request_id}] Sending command {command_id} with response to device {translation_result.device_id}")

                # Acknowledge command delivery
                if pending_commands:
                    self.command_consumer.acknowledge_command(translation_result.device_id, command_id)

                # Create CoAP response with both command and response
                # The response_bytes contain the manufacturer-specific response data
                response_payload = command_bytes + response_bytes if response_bytes else command_bytes

                return aiocoap.Message(
                    mtype=aiocoap.ACK,
                    code=aiocoap.Code.CREATED,
                    token=request.token,
                    payload=response_payload
                )

        except Exception as e:
            logger.error(f"[{request_id}] Error handling command with response: {e}")

    # ... rest of existing code ...
```

## Configuration Changes

### Parser Script Registration

Update the connector configuration to ensure all parsers implement the response interface:

```yaml
connectors:
  - connector_id: "coap-connector"
    translators:
      - type: "protobuf"
        config:
          response_generation:
            enabled: true
            default_response_type: "time_sync" # For Efento
            validation:
              required_functions: ["format_command", "format_response"]
```

## Benefits

### 1. **Architectural Consistency**

- Device-specific logic remains in parser scripts
- Command consumer stays device-agnostic
- Clear separation of concerns maintained

### 2. **Flexibility**

- Each manufacturer can implement custom response logic
- Generic parsers can use standard CoAP ACK
- Easy to add new response types

### 3. **Performance**

- Responses pre-generated during command processing
- No runtime response generation overhead
- Efficient command delivery

### 4. **Maintainability**

- Response logic co-located with command formatting
- Single parser file per manufacturer
- Easy to test and debug

## Migration Path

### Phase 1: Interface Extension

1. Add `format_response()` to existing parser scripts
2. Implement default (empty) response for generic devices
3. Implement Efento-specific response logic

### Phase 2: Command Consumer Updates

1. Modify command storage to include response data
2. Update command formatting to generate responses
3. Enhance command retrieval methods

### Phase 3: DataRootResource Integration

1. Update response handling to use generated response data
2. Implement combined command + response payload
3. Add enhanced logging and monitoring

### Phase 4: Testing & Validation

1. Test with existing Efento devices
2. Validate generic device compatibility
3. Performance testing with high command volumes

## Error Handling

### Response Generation Failures

- Fall back to standard CoAP ACK if response generation fails
- Log detailed error information for debugging
- Maintain command delivery even if response fails

### Backward Compatibility

- Support parsers without `format_response()` function
- Graceful degradation to current behavior
- Migration warnings for deprecated parser interfaces

## Monitoring & Observability

### New Metrics

- Response generation success rate
- Response payload sizes
- Command+response delivery latency
- Parser response cache hit rates

### Enhanced Logging

- Response generation details
- Combined payload sizes
- Manufacturer-specific response metadata
- Error traces for response failures

---

**Decision Required**: Should we proceed with this parser-based response generation approach, or would you prefer an alternative strategy?
