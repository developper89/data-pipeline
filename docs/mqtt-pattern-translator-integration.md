# MQTT Connector Pattern Translator Integration

## Overview

The MQTT connector has been updated to use the protocol-agnostic pattern translator for device ID extraction, replacing the previous hardcoded topic parsing logic.

## Key Changes

### 1. Enhanced RawData Model

- Added `path` field to support protocol-agnostic patterns (MQTT topics, CoAP paths, HTTP URLs, etc.)
- Added `raw_data` and `translator_type` fields to TranslationResult
- Simplified to use only `path` field for all protocols instead of separate `topic` and `path` fields

### 2. Updated MQTT Client (`connectors/mqtt_connector/client.py`)

- **Translation Manager Integration**: Loads translator configurations from YAML
- **Dynamic Device ID Extraction**: Uses pattern translator instead of hardcoded logic
- **Fallback Handling**: Falls back to topic-based ID if translation fails
- **Enhanced Logging**: Detailed logging for device ID extraction process

### 3. Configuration-Driven Approach

The MQTT connector now reads translator configuration from `shared/connectors_config.yaml`:

```yaml
- connector_id: "mqtt_preservarium"
  translators:
    - type: "pattern"
      config:
        device_id_extraction:
          sources:
            - type: "path_pattern"
              pattern: "broker_data/{device_id}/data"
              priority: 1
              validation:
                min_length: 3
                max_length: 50
                pattern: "^[a-fA-F0-9]+$"
```

### 4. Protocol-Agnostic Architecture

- **Path Pattern Support**: Extracts from MQTT topics using configurable patterns
- **JSON Payload Support**: Can extract device IDs from JSON message content
- **Validation Rules**: Configurable validation per extraction source
- **Priority System**: Multiple extraction sources with priority ordering

## Implementation Details

### Device ID Extraction Flow

1. **Message Reception**: MQTT message received with topic and payload
2. **RawData Creation**: Convert to RawData object with `path` field set to MQTT topic
3. **Translation Manager**: Uses configured pattern translator
4. **Pattern Matching**: Attempts each configured extraction source by priority
5. **Validation**: Applies validation rules (length, regex patterns)
6. **Fallback**: Uses topic as device ID if extraction fails

### Key Methods

#### `_initialize_translation_manager()`

- Loads translator configurations for `mqtt_preservarium` connector
- Creates TranslatorFactory instances
- Returns initialized TranslationManager

#### `_extract_device_id_using_translation(topic, payload_bytes)`

- Creates RawData object with MQTT topic as `path` field and payload
- Uses TranslationManager for device ID extraction
- Provides fallback mechanism for failed extractions

### Error Handling

- **Configuration Errors**: Graceful fallback if translator config is missing
- **Translation Failures**: Falls back to topic-based device ID generation
- **Validation Failures**: Logs validation errors and tries next source
- **Exception Handling**: Comprehensive error catching with detailed logging

## Benefits

### 1. **Flexibility**

- No more hardcoded topic parsing
- Easily configurable extraction patterns
- Support for multiple MQTT topic formats

### 2. **Maintainability**

- Centralized configuration in YAML
- Shared translation logic across connectors
- Clear separation of concerns

### 3. **Extensibility**

- Easy to add new extraction patterns
- JSON payload extraction capability
- Protocol-agnostic design for future use

### 4. **Robustness**

- Multiple extraction sources with priorities
- Validation rules prevent invalid device IDs
- Fallback mechanisms ensure operation continuity

## Example Configurations

### Multiple Pattern Support

```yaml
device_id_extraction:
  sources:
    # Legacy topic format
    - type: "path_pattern"
      pattern: "broker_data/{device_id}/data"
      priority: 1

    # New topic format
    - type: "path_pattern"
      pattern: "sensors/{device_id}/measurements"
      priority: 2

    # JSON payload fallback
    - type: "json_payload"
      json_path: "$.device.id"
      fallback_paths: ["$.deviceId", "$.id"]
      priority: 3
```

### Validation Rules

```yaml
- type: "path_pattern"
  pattern: "devices/{device_id}/status"
  validation:
    min_length: 3
    max_length: 50
    pattern: "^[a-fA-F0-9]+$" # Hex only
```

## Testing

The integration can be tested by:

1. Configuring different topic patterns in YAML
2. Publishing MQTT messages with various topic formats
3. Observing device ID extraction in logs
4. Verifying fallback behavior for unmatched patterns

## Migration Notes

### From Old Implementation

- **Before**: Hardcoded parsing of `broker_data/{device_id}/data` pattern
- **After**: Configurable patterns supporting multiple formats
- **Compatibility**: Original pattern still supported via configuration

### Configuration Changes

- Translator configuration moved to `shared/connectors_config.yaml`
- Pattern definitions now use generic `path_pattern` type
- Validation rules are more flexible and configurable
