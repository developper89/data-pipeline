# CoAP Connector Architecture

## Overview

The CoAP connector is a sophisticated, modular system designed to handle bidirectional communication with IoT devices using the CoAP (Constrained Application Protocol) protocol. It features a flexible translation layer that enables device-agnostic message processing while supporting device-specific protocol implementations.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Components](#core-components)
3. [Translation System](#translation-system)
4. [Data Flow](#data-flow)
5. [Parser Integration](#parser-integration)
6. [Configuration System](#configuration-system)
7. [Error Handling & Resilience](#error-handling--resilience)
8. [Performance Considerations](#performance-considerations)
9. [Examples](#examples)
10. [Troubleshooting](#troubleshooting)

## Architecture Overview

The CoAP connector follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    CoAP Devices                              │
└─────────────────────┬───────────────────────────────────────┘
                      │ CoAP Protocol
┌─────────────────────▼───────────────────────────────────────┐
│                CoAP Connector                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │   CoAP      │ │  Command    │ │    Translation          │ │
│  │  Server     │ │  Consumer   │ │     Manager             │ │
│  └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │ Kafka Protocol
┌─────────────────────▼───────────────────────────────────────┐
│                 Kafka Message Bus                            │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ iot_raw_data│ │device_commands│ │   Other Topics          │ │
│  └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│              Downstream Services                             │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ Normalizer  │ │  Validator  │ │     Other Services      │ │
│  └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. DataRootResource (`resources.py`)

**Purpose**: Main CoAP request handler that processes incoming device communications.

**Key Responsibilities**:

- Receives and parses CoAP requests from devices
- Manages CoAP resource discovery (`.well-known/core`)
- Integrates with translation system for device ID extraction
- Publishes raw messages to Kafka
- Handles bidirectional communication (responses with commands)

**Key Methods**:

- `render()`: Main request processing method
- `_extract_device_id_using_translation()`: Device ID extraction
- `_generate_well_known_core_response()`: CoAP discovery support

**Integration Points**:

- Uses `TranslationManager` for device ID extraction
- Interfaces with `CommandConsumer` for pending commands
- Publishes via `KafkaProducer`

### 2. CommandConsumer (`command_consumer.py`)

**Purpose**: Manages outgoing commands to devices with parser-based formatting.

**Key Responsibilities**:

- Consumes commands from Kafka `device_commands` topic
- Formats commands using device-specific parsers
- Stores formatted commands in memory until delivery
- Acknowledges successful command delivery

**Key Methods**:

- `_process_command()`: Main command processing
- `_format_command_with_parser()`: Parser-based command formatting
- `get_formatted_command()`: Retrieves pending commands for devices
- `acknowledge_command()`: Marks commands as delivered

**Integration Points**:

- Uses `AsyncResilientKafkaConsumer` for reliability
- Integrates with translation system for command formatting
- Interfaces with parser scripts for device-specific formatting

### 3. TranslationManager (`shared/translation/manager.py`)

**Purpose**: Orchestrates multiple translators for device ID extraction.

**Key Responsibilities**:

- Manages translator instances with priority ordering
- Delegates device ID extraction to appropriate translator
- Provides fallback mechanisms for translation failures

**Key Methods**:

- `extract_device_id()`: Main translation coordination
- `add_translator()`: Runtime translator management

### 4. KafkaProducer (`kafka_producer.py`)

**Purpose**: Publishes processed messages to Kafka topics.

**Key Responsibilities**:

- Serializes messages to JSON format
- Handles Kafka connection management
- Provides error handling for message publishing

## Translation System

### Architecture

The translation system uses a pluggable architecture supporting multiple translator types:

```
┌─────────────────────────────────────────────────────────────┐
│                 Translation Manager                          │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Translator Factory                          │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐ │ │
│  │  │ Protobuf    │ │  Pattern    │ │    Future           │ │ │
│  │  │ Translator  │ │ Translator  │ │   Translators       │ │ │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Translator Types

#### 1. ProtobufTranslator

**Purpose**: Handles protobuf-based device communication (e.g., Efento sensors).

**Key Features**:

- Dynamic protobuf schema loading
- Path-based message type mapping
- Priority-based device ID extraction
- Command formatting support

**Configuration Example**:

```yaml
type: "protobuf"
config:
  manufacturer: "efento"
  path_mapping:
    "i": "device_info"
    "m": "measurements"
    "c": "config"
  device_id_extraction:
    sources:
      - message_type: "measurements"
        field_path: "serial_num"
        priority: 2
```

#### 2. PatternTranslator

**Purpose**: Handles pattern-based device ID extraction from paths and JSON payloads.

**Key Features**:

- Path pattern matching (MQTT topics, CoAP paths)
- JSON payload extraction using JSONPath
- Multi-source extraction with priorities
- Validation and normalization

**Configuration Example**:

```yaml
type: "pattern"
config:
  device_id_extraction:
    sources:
      - type: "path_pattern"
        pattern: "broker_data/+/{device_id}/data"
        priority: 1
```

### Data Models

#### RawData

```python
@dataclass
class RawData:
    payload_bytes: bytes
    protocol: str
    metadata: Dict[str, Any] = None
    path: Optional[str] = None
```

#### TranslationResult

```python
@dataclass
class TranslationResult:
    success: bool
    device_id: Optional[str] = None
    translator_used: Optional[str] = None
    translator: Optional['BaseTranslator'] = None
    metadata: Dict[str, Any] = None
    error: Optional[str] = None
```

## Data Flow

### Incoming Data Flow

1. **CoAP Request Reception**

   - Device sends CoAP request with protobuf payload
   - `DataRootResource.render()` processes request

2. **Translation Layer Processing**

   - Creates `RawData` object from CoAP request
   - `TranslationManager.extract_device_id()` tries translators by priority
   - First successful translator provides device ID

3. **Message Creation & Publishing**

   - Creates `RawMessage` with extracted device ID
   - Publishes to Kafka `iot_raw_data` topic

4. **Command Response (if applicable)**
   - Checks for pending commands for the device
   - Returns formatted command in CoAP response

### Outgoing Command Flow

1. **Command Reception**

   - `CommandConsumer` receives from Kafka `device_commands` topic
   - Parses into `CommandMessage` model

2. **Command Formatting**

   - Uses translator system to get device-specific parser
   - Calls parser's `format_command()` function
   - Creates binary protobuf command

3. **Command Storage**

   - Stores formatted command in memory
   - Indexed by device ID for quick lookup

4. **Command Delivery**
   - When device contacts server, retrieves formatted command
   - Returns command in CoAP response payload
   - Acknowledges successful delivery

### Data Flow Diagram

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   CoAP      │    │Translation  │    │   Kafka     │
│  Device     │───▶│  Manager    │───▶│ Publisher   │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       │                   ▼                   ▼
       │           ┌─────────────┐    ┌─────────────┐
       │           │  Protobuf   │    │ Raw Message │
       │           │ Translator  │    │   Topic     │
       │           └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Command    │    │   Parser    │    │ Normalizer  │
│  Response   │◄───│   Script    │◄───│  Service    │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Parser Integration

### Parser Script Architecture

Parser scripts serve as the bridge between the generic translation system and device-specific protocol implementations.

#### Dual-Purpose Design

**1. Incoming Data Processing** (`parse` function)

```python
def parse(payload: bytes, translator: Any, metadata: dict, config: dict) -> List[dict]:
    """
    Parses raw payload using translator and normalizes to StandardizedOutput format.
    """
```

**2. Outgoing Command Processing** (`format_command` function)

```python
def format_command(command: dict, translator: Any, config: dict) -> bytes:
    """
    Formats commands for device transmission using protobuf translator.
    """
```

### Integration Points

#### Leverages Translation System

- Uses `translator.message_parser.parse_message_to_dict()` for protobuf decoding
- Accesses translator's protobuf classes for command formatting
- Inherits configuration from translator (path mappings, message types)

#### Provides Device-Specific Logic

- Complex measurement normalization (accumulators, time-series)
- Command formatting with device-specific encoding
- Error handling and validation

### Example: Efento Parser Integration

```python
# Incoming data processing
parsed_message_type, decoded_data = translator.message_parser.parse_message_to_dict(payload)

if parsed_message_type == "measurements":
    normalized_output_list = _normalize_measurements(decoded_data, metadata)
elif parsed_message_type == "device_info":
    normalized_output_list = _normalize_device_info(decoded_data, metadata)

# Outgoing command processing
proto_class = translator.message_parser.proto_modules["config"]["class"]
proto_message = proto_class()
# ... populate message fields ...
binary_config = proto_message.SerializeToString()
```

## Configuration System

### YAML Configuration Structure

The system uses centralized YAML configuration for connector and translator settings:

```yaml
connectors:
  - connector_id: "coap-connector"
    image: "coap-connector:latest"
    translators:
      - type: "protobuf"
        config:
          manufacturer: "efento"
          path_mapping:
            "i": "device_info"
            "m": "measurements"
            "c": "config"
          device_id_extraction:
            sources:
              - message_type: "measurements"
                field_path: "serial_num"
                priority: 2
          command_formatting:
            sources:
              - command_type: "alarm"
                message_type: "config"
                priority: 1
        priority: 1
    env:
      COAP_HOST: "0.0.0.0"
      COAP_PORT: "5683"
      KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
```

### Configuration Loading

1. **Startup Configuration**

   - `ConfigLoader` reads YAML file
   - Validates configuration structure
   - Creates translator instances via `TranslatorFactory`

2. **Runtime Configuration**

   - Path mappings used for message type detection
   - Device ID extraction sources prioritized
   - Command formatting rules applied

3. **Environment Variables**
   - CoAP server settings
   - Kafka connection parameters
   - Logging configuration

## Error Handling & Resilience

### Translation Layer Resilience

1. **Multiple Translator Support**

   - Priority-based translator selection
   - Fallback to next translator on failure
   - Graceful degradation with error logging

2. **Validation & Normalization**
   - Input validation at each stage
   - Consistent error message formats
   - Metadata preservation for debugging

### Kafka Resilience

1. **AsyncResilientKafkaConsumer**

   - Automatic reconnection on failures
   - Offset management and replay
   - Dead letter queue handling

2. **Message Publishing**
   - Retry mechanisms for failed publishes
   - Circuit breaker patterns
   - Monitoring and alerting

### CoAP Protocol Resilience

1. **Request Processing**

   - Timeout handling for slow devices
   - Malformed message rejection
   - Resource discovery fallbacks

2. **Command Delivery**
   - Command expiration handling
   - Delivery acknowledgment tracking
   - Retry logic for failed commands

## Performance Considerations

### Memory Management

1. **Streaming Processing**

   - Minimal buffering of large payloads
   - Immediate processing and forwarding
   - Efficient protobuf parsing

2. **Command Storage**
   - In-memory pending command storage
   - Configurable expiration policies
   - Memory usage monitoring

### Processing Efficiency

1. **Translation Optimization**

   - Cached translator instances
   - Lazy loading of protobuf schemas
   - Efficient device ID extraction

2. **Concurrent Processing**
   - Async/await throughout the pipeline
   - Non-blocking I/O operations
   - Configurable thread pools

### Monitoring & Metrics

1. **Request Tracking**

   - Unique request IDs for tracing
   - Processing time measurements
   - Success/failure rate monitoring

2. **Resource Usage**
   - Memory and CPU usage tracking
   - Connection pool monitoring
   - Kafka topic lag monitoring

## Examples

### Device Registration Flow

```python
# 1. Device sends device_info message
coap_request = {
    "method": "POST",
    "uri_path": ["i"],  # Maps to device_info
    "payload": protobuf_device_info_bytes
}

# 2. Translation extracts device ID
raw_data = RawData(
    payload_bytes=protobuf_device_info_bytes,
    protocol="coap",
    path="i"
)
result = translation_manager.extract_device_id(raw_data)

# 3. Raw message published to Kafka
raw_message = RawMessage(
    device_id=result.device_id,
    payload_hex=protobuf_device_info_bytes.hex(),
    protocol="coap",
    metadata={"uri_path": "i", "message_type": "device_info"}
)
```

### Command Formatting Example

```python
# 1. Command received from Kafka
command_message = CommandMessage(
    device_id="abc123",
    command_type="alarm",
    payload={
        "threshold": 25.5,
        "math_operator": ">",
        "field_label": "T_int",
        "level": "WARNING"
    }
)

# 2. Parser formats command
formatted_command = parser.format_command(
    command=command_message.dict(),
    translator=protobuf_translator,
    config={}
)

# 3. Command stored until device contacts server
command_consumer.store_formatted_command(
    device_id="abc123",
    command_data=formatted_command
)
```

## Troubleshooting

### Common Issues

1. **Translation Failures**

   - **Symptom**: Device ID not extracted
   - **Cause**: Misconfigured translator or unknown message format
   - **Solution**: Check translator configuration and protobuf schemas

2. **Command Formatting Errors**

   - **Symptom**: Commands not delivered to devices
   - **Cause**: Parser script errors or invalid protobuf encoding
   - **Solution**: Validate parser script and command payload format

3. **Kafka Connection Issues**
   - **Symptom**: Messages not published/consumed
   - **Cause**: Network connectivity or configuration issues
   - **Solution**: Check Kafka bootstrap servers and topic configuration

### Debugging Tools

1. **Logging Configuration**

   ```python
   # Enable debug logging for translation
   logging.getLogger("shared.translation").setLevel(logging.DEBUG)

   # Enable debug logging for parser
   logging.getLogger("parser_script").setLevel(logging.DEBUG)
   ```

2. **Request Tracing**

   - Each request gets unique ID for end-to-end tracing
   - Metadata preserved throughout processing pipeline
   - Detailed logging at each processing stage

3. **CoAP Discovery**
   - Use `.well-known/core` endpoint to verify server configuration
   - Check available resources and their capabilities
   - Validate path mappings and resource types

### Performance Monitoring

1. **Key Metrics**

   - Request processing time
   - Translation success rate
   - Command delivery rate
   - Memory usage patterns

2. **Alerting**
   - Failed translation attempts
   - Kafka connection failures
   - High memory usage warnings
   - Command delivery failures

## Future Enhancements

1. **Persistent Command Storage**

   - Replace in-memory storage with persistent queue
   - Support for command prioritization
   - Command retry mechanisms

2. **Advanced Translation Features**

   - Machine learning-based device identification
   - Dynamic translator loading
   - Multi-protocol support

3. **Enhanced Monitoring**
   - Real-time dashboards
   - Predictive failure detection
   - Performance optimization recommendations

---

_This document provides a comprehensive overview of the CoAP connector architecture. For implementation details, refer to the individual component documentation and source code._
