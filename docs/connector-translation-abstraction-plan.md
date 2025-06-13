# Connector Translation Abstraction Plan

**Date**: December 2024  
**Version**: 1.0  
**Purpose**: Abstract manufacturer-specific device_id extraction logic from protocol connectors

## 1. Problem Analysis

### Current Issues

- **Tight Coupling**: Device ID extraction logic is hardcoded in each connector
- **Manufacturer Dependency**: MQTT topic structures and CoAP payload formats vary by manufacturer
- **No Flexibility**: Adding new manufacturers requires connector code changes
- **Duplication**: Similar extraction patterns are implemented multiple times

### Current Implementation

```python
# MQTT Connector - Hardcoded topic parsing
topic_parts = msg.topic.split('/')
if len(topic_parts) >= 2 and topic_parts[0] == 'broker_data':
    device_id = topic_parts[2]

# CoAP Connector - Hardcoded protobuf parsing
device_id = self._parse_protobuf_field(payload, field_number=1)
```

## 2. Proposed Architecture

### 2.1 Translation Layer Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Connector     │    │  Translation    │    │    Normalizer   │
│   (Protocol)    │───▶│     Layer       │───▶│                 │
│                 │    │ (Manufacturer)  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 2.2 Key Principles

1. **Separation of Concerns**: Protocol logic vs manufacturer-specific parsing
2. **Configuration-Driven**: Translation rules defined in YAML
3. **Pluggable**: Easy to add new manufacturer translators
4. **Fallback Mechanisms**: Multiple extraction strategies per translator

## 3. Implementation Design

### 3.1 Translation Manager

```python
class TranslationManager:
    def __init__(self, translator_configs: List[TranslatorConfig]):
        self.translators = self._load_translators(translator_configs)

    def extract_device_id(self, raw_data: RawData) -> Optional[str]:
        for translator in self.translators:
            if translator.can_handle(raw_data):
                return translator.extract_device_id(raw_data)
        return None
```

### 3.2 Translator Interface

```python
class BaseTranslator(ABC):
    @abstractmethod
    def can_handle(self, raw_data: RawData) -> bool:
        """Check if this translator can handle the data format"""
        pass

    @abstractmethod
    def extract_device_id(self, raw_data: RawData) -> Optional[str]:
        """Extract device ID from the raw data"""
        pass
```

### 3.3 Configuration Structure

```yaml
connectors:
  - connector_id: "mqtt_preservarium"
    image: "mqtt-connector:latest"
    translators:
      - type: "mqtt_topic_pattern"
        config:
          pattern: "broker_data/{device_id}/data"
          priority: 1
      - type: "mqtt_topic_fallback"
        config:
          extract_from: "topic"
          priority: 2
    env:
      BROKER_HOST: "preservarium.fr"
      # ... other env vars
```

## 4. Translator Types

### 4.1 MQTT Translators

#### Topic Pattern Translator

```yaml
type: "mqtt_topic_pattern"
config:
  pattern: "broker_data/{device_id}/data"
  validation:
    device_id_regex: "^[a-zA-Z0-9_-]+$"
    min_length: 3
    max_length: 50
```

#### Payload JSON Translator

```yaml
type: "mqtt_payload_json"
config:
  json_path: "$.device.id"
  fallback_paths: ["$.deviceId", "$.id"]
```

### 4.2 CoAP Translators

#### Protobuf Field Translator

```yaml
type: "coap_protobuf_field"
config:
  primary_field: 1
  fallback_fields: [16, 20]
  encoding: "utf-8"
  hex_fallback: true
```

#### URL Path Translator

```yaml
type: "coap_url_path"
config:
  pattern: "/data/{device_id}"
  position: 1 # 0-indexed path segment
```

## 5. Implementation Steps

### Phase 1: Foundation (Week 1-2)

1. Create shared translation framework
2. Define translator interfaces and base classes
3. Update RawMessage to include translation metadata
4. Create translation manager service

### Phase 2: MQTT Implementation (Week 3)

1. Implement MQTT topic pattern translator
2. Implement MQTT payload JSON translator
3. Update MQTT connector to use translation layer
4. Add configuration support to manager service

### Phase 3: CoAP Implementation (Week 4)

1. Implement CoAP protobuf field translator
2. Implement CoAP URL path translator
3. Update CoAP connector to use translation layer
4. Refactor existing device_id extraction logic

### Phase 4: Testing & Validation (Week 5)

1. Comprehensive testing with existing data
2. Performance benchmarking
3. Error handling validation
4. Documentation updates

## 6. Directory Structure

```
shared/
├── translation/
│   ├── __init__.py
│   ├── manager.py
│   ├── base.py
│   ├── mqtt/
│   │   ├── __init__.py
│   │   ├── topic_pattern.py
│   │   └── payload_json.py
│   └── coap/
│       ├── __init__.py
│       ├── protobuf_field.py
│       └── url_path.py
└── models/
    └── translation.py
```

## 7. Migration Strategy

### 7.1 Backward Compatibility

- Keep existing extraction logic as default fallback
- Gradual migration per connector
- No breaking changes to downstream services

### 7.2 Configuration Migration

```yaml
# Old format (deprecated but supported)
connectors:
  - connector_id: "mqtt_legacy"
    # ... existing config

# New format (recommended)
connectors:
  - connector_id: "mqtt_modern"
    translators:
      - type: "mqtt_topic_pattern"
        config:
          pattern: "sensors/{device_id}/data"
```

## 8. Benefits

### 8.1 For Development

- **Reduced Code Duplication**: Common extraction patterns reused
- **Easier Testing**: Isolated translation logic
- **Faster Development**: New manufacturers added via configuration

### 8.2 For Operations

- **Runtime Configuration**: Update extraction rules without code changes
- **Multiple Strategies**: Fallback mechanisms for robust extraction
- **Better Monitoring**: Translation success/failure metrics

### 8.3 For Scalability

- **Manufacturer Independence**: Add new formats without connector changes
- **Protocol Agnostic**: Translation patterns work across protocols
- **Performance Optimization**: Caching and optimization opportunities

## 9. Error Handling

### 9.1 Translation Failures

```python
class TranslationResult:
    device_id: Optional[str]
    success: bool
    error: Optional[str]
    translator_used: Optional[str]
    fallback_attempts: List[str]
```

### 9.2 Fallback Strategy

1. Try primary translator
2. Try configured fallback translators in priority order
3. Use protocol-specific default extraction
4. Generate synthetic device_id if configured
5. Reject message if no extraction possible

## 10. Monitoring & Metrics

### 10.1 Translation Metrics

- Translation success rate by translator type
- Device_id extraction time
- Fallback usage frequency
- Unknown format detection

### 10.2 Alerting

- High translation failure rate
- New unknown data formats detected
- Performance degradation alerts

## 11. Future Enhancements

### 11.1 Machine Learning Integration

- Automatic pattern detection for unknown formats
- Confidence scoring for extracted device_ids
- Adaptive fallback ordering based on success rates

### 11.2 Advanced Features

- Device_id normalization and validation
- Cross-protocol device_id mapping
- Manufacturer auto-detection based on data patterns

## 12. Implementation Priority

### High Priority

1. MQTT topic pattern translator
2. CoAP protobuf field translator
3. Configuration system integration

### Medium Priority

1. JSON payload translators
2. URL path translators
3. Advanced error handling

### Low Priority

1. Machine learning features
2. Performance optimizations
3. Advanced monitoring dashboards
