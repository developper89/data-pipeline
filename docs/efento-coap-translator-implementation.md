# Generic Protobuf Translator Implementation Plan

**Date**: December 2024  
**Version**: 2.0  
**Purpose**: Generic protobuf translator with manufacturer-specific configuration for CoAP devices

## 1. Overview

Instead of creating manufacturer-specific translators, we implement a generic ProtobufTranslator that:

- Handles standard protobuf parsing logic
- Takes manufacturer-specific configuration (proto schemas, field mappings)
- Supports multiple manufacturers through configuration
- Manages proto schema compilation and device ID extraction

## 2. Redesigned Architecture

### 2.1 Directory Structure

```
shared/
├── translation/
│   ├── protobuf/
│   │   ├── __init__.py
│   │   ├── translator.py           # Generic protobuf translator
│   │   ├── proto_loader.py         # Loads pre-compiled protobuf modules
│   │   ├── message_parser.py       # Generic protobuf parsing
│   │   ├── device_id_extractor.py  # Generic device ID extraction
│   │   └── proto_schemas/          # Pre-compiled protobuf modules
│   │       ├── efento/
│   │       │   ├── __init__.py
│   │       │   ├── proto_measurements_pb2.py    # Pre-compiled
│   │       │   ├── proto_device_info_pb2.py     # Pre-compiled
│   │       │   └── proto_config_pb2.py          # Pre-compiled
│   │       └── manufacturer_x/
│   │           ├── __init__.py
│   │           └── device_data_pb2.py           # Pre-compiled
│   └── coap/
│       └── protobuf_translator.py  # CoAP-specific wrapper
```

### 2.2 Configuration Schema

```yaml
type: "coap_protobuf"
config:
  manufacturer: "efento"

  # Device ID extraction configuration
  device_id_extraction:
    sources:
      - message_type: "measurements"
        field_path: "serial_number"
        priority: 1
      - message_type: "device_info"
        field_path: "imei"
        priority: 2

    validation:
      regex: "^(IMEI:)?[0-9]{15}$"
      normalize: true
      remove_prefix: "IMEI:"

  # Message type detection
  message_types:
    measurements:
      proto_class: "Measurements"
      proto_module: "proto_measurements_pb2"
      required_fields: ["serial_number", "timestamp"]
    device_info:
      proto_class: "DeviceInfo"
      proto_module: "proto_device_info_pb2"
      required_fields: ["imei"]
    config:
      proto_class: "Config"
      proto_module: "proto_config_pb2"
      required_fields: []
```

## 3. Core Implementation

### 3.1 Generic Protobuf Translator

```python
# shared/translation/protobuf/translator.py
from typing import Optional, Dict, Any
import logging
from shared.translation.base import BaseTranslator
from shared.models.translation import RawData, TranslationResult
from .message_parser import ProtobufMessageParser
from .device_id_extractor import ProtobufDeviceIdExtractor

logger = logging.getLogger(__name__)

class ProtobufTranslator(BaseTranslator):
    """Generic protobuf translator that works with any manufacturer's schemas."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.manufacturer = config.get('manufacturer', 'unknown')

        # Initialize components with manufacturer-specific config
        self.message_parser = ProtobufMessageParser(
            manufacturer=self.manufacturer,
            message_types=config.get('message_types', {})
        )

        self.device_id_extractor = ProtobufDeviceIdExtractor(
            config.get('device_id_extraction', {})
        )

    def can_handle(self, raw_data: RawData) -> bool:
        """Check if this translator can handle the protobuf data."""
        if raw_data.protocol != "coap":
            return False

        # Quick protobuf format detection
        if not self._is_likely_protobuf(raw_data.payload_bytes):
            return False

        # Try to parse with manufacturer's schemas
        try:
            message_type = self.message_parser.detect_message_type(raw_data.payload_bytes)
            return message_type is not None
        except Exception:
            return False

    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """Extract device ID from protobuf payload."""
        try:
            # Parse the protobuf message
            message_type, parsed_message = self.message_parser.parse_message(
                raw_data.payload_bytes
            )

            # Extract device ID using configured field mappings
            device_id = self.device_id_extractor.extract(message_type, parsed_message)

            if device_id:
                return TranslationResult(
                    device_id=device_id,
                    success=True,
                    translator_used=f"protobuf_{self.manufacturer}",
                    metadata={
                        "manufacturer": self.manufacturer,
                        "message_type": message_type,
                        "extraction_source": self.device_id_extractor.last_source_used
                    }
                )
            else:
                return TranslationResult(
                    success=False,
                    error=f"No device ID found in {self.manufacturer} protobuf message"
                )

        except Exception as e:
            logger.exception(f"Protobuf parsing failed for {self.manufacturer}: {e}")
            return TranslationResult(
                success=False,
                error=f"Protobuf parsing error: {str(e)}"
            )

    def _is_likely_protobuf(self, payload: bytes) -> bool:
        """Quick check if payload looks like protobuf."""
        if len(payload) < 2:
            return False
        # Protobuf typically starts with field tags
        return payload[0] in [0x08, 0x0a, 0x10, 0x12, 0x18, 0x1a, 0x20, 0x22]
```

### 3.2 Proto Module Loader

```python
# shared/translation/protobuf/proto_loader.py
import importlib
from typing import Any

class ProtoModuleLoader:
    """Loads pre-compiled protobuf modules for different manufacturers."""

    @staticmethod
    def load_proto_module(manufacturer: str, module_name: str) -> Any:
        """Load a compiled protobuf module for the given manufacturer."""
        module_path = f"shared.translation.protobuf.proto_schemas.{manufacturer}.{module_name}"
        return importlib.import_module(module_path)

    @staticmethod
    def get_proto_class(manufacturer: str, module_name: str, class_name: str) -> Any:
        """Get a specific protobuf class from a module."""
        module = ProtoModuleLoader.load_proto_module(manufacturer, module_name)
        return getattr(module, class_name)
```

### 3.3 Generic Message Parser

```python
# shared/translation/protobuf/message_parser.py
from typing import Optional, Tuple, Any, Dict
import logging
from .proto_loader import ProtoModuleLoader

logger = logging.getLogger(__name__)

class ProtobufMessageParser:
    """Generic protobuf message parser that works with any manufacturer's schemas."""

    def __init__(self, manufacturer: str, message_types: Dict[str, Any]):
        self.manufacturer = manufacturer
        self.message_types = message_types
        self._load_proto_modules()

    def _load_proto_modules(self):
        """Load compiled protobuf modules for this manufacturer."""
        self.proto_modules = {}

        for message_type, config in self.message_types.items():
            try:
                proto_module = config.get('proto_module')
                proto_class = config.get('proto_class')

                if proto_module and proto_class:
                    cls = ProtoModuleLoader.get_proto_class(
                        self.manufacturer, proto_module, proto_class
                    )

                    self.proto_modules[message_type] = {
                        'class': cls,
                        'required_fields': config.get('required_fields', [])
                    }
            except ImportError as e:
                logger.warning(f"Failed to load {message_type} for {self.manufacturer}: {e}")
                continue

    def detect_message_type(self, payload: bytes) -> Optional[str]:
        """Detect the type of protobuf message."""
        for message_type, proto_info in self.proto_modules.items():
            if self._try_parse_message(payload, proto_info):
                return message_type
        return None

    def parse_message(self, payload: bytes) -> Tuple[str, Any]:
        """Parse protobuf message and return type and parsed object."""
        message_type = self.detect_message_type(payload)

        if message_type and message_type in self.proto_modules:
            proto_info = self.proto_modules[message_type]
            message = proto_info['class']()
            message.ParseFromString(payload)
            return message_type, message
        else:
            raise ValueError(f"Unknown {self.manufacturer} message type")

    def _try_parse_message(self, payload: bytes, proto_info: Dict) -> bool:
        """Try to parse payload as specific message type."""
        try:
            message = proto_info['class']()
            message.ParseFromString(payload)

            # Validate required fields exist
            required_fields = proto_info.get('required_fields', [])
            for field in required_fields:
                if not hasattr(message, field):
                    return False

            return True
        except Exception:
            return False
```

### 3.4 Generic Device ID Extractor

```python
# shared/translation/protobuf/device_id_extractor.py
import re
from typing import Optional, Any, Dict, List

class ProtobufDeviceIdExtractor:
    """Generic device ID extractor that works with configured field mappings."""

    def __init__(self, extraction_config: Dict[str, Any]):
        self.sources = extraction_config.get('sources', [])
        self.validation = extraction_config.get('validation', {})
        self.last_source_used = None

        # Sort sources by priority
        self.sources = sorted(self.sources, key=lambda x: x.get('priority', 999))

    def extract(self, message_type: str, parsed_message: Any) -> Optional[str]:
        """Extract device ID from parsed message using configured sources."""

        # Try configured sources in priority order
        for source in self.sources:
            if source.get('message_type') != message_type:
                continue

            field_path = source.get('field_path')
            device_id = self._extract_from_field_path(parsed_message, field_path)

            if device_id and self._validate_device_id(device_id):
                self.last_source_used = f"{message_type}.{field_path}"
                return self._normalize_device_id(device_id)

        return None

    def _extract_from_field_path(self, message: Any, field_path: str) -> Optional[str]:
        """Extract value from nested field path (e.g., 'device.info.id')."""
        try:
            current = message
            for field in field_path.split('.'):
                if hasattr(current, field):
                    current = getattr(current, field)
                else:
                    return None

            # Convert to string
            if isinstance(current, (str, bytes)):
                return current.decode('utf-8') if isinstance(current, bytes) else current
            return str(current)

        except Exception:
            return None

    def _validate_device_id(self, device_id: str) -> bool:
        """Validate device ID using configured regex."""
        if not device_id or len(device_id) < 3:
            return False

        regex_pattern = self.validation.get('regex')
        if regex_pattern:
            return bool(re.match(regex_pattern, device_id))

        # Default validation for common formats
        return bool(re.match(r'^[a-zA-Z0-9:_-]{3,32}$', device_id))

    def _normalize_device_id(self, device_id: str) -> str:
        """Normalize device ID using configured rules."""
        # Remove configured prefix
        prefix_to_remove = self.validation.get('remove_prefix')
        if prefix_to_remove and device_id.startswith(prefix_to_remove):
            device_id = device_id[len(prefix_to_remove):]

        # Apply normalization
        if self.validation.get('normalize', True):
            device_id = device_id.lower()

        return device_id
```

## 4. Efento Configuration Example

### 4.1 Efento-Specific Configuration

```yaml
type: "coap_protobuf"
config:
  manufacturer: "efento"

  device_id_extraction:
    sources:
      - message_type: "measurements"
        field_path: "serial_number"
        priority: 1
      - message_type: "device_info"
        field_path: "imei"
        priority: 2

    validation:
      regex: "^(IMEI:)?[0-9]{15}$"
      normalize: true
      remove_prefix: "IMEI:"

  message_types:
    measurements:
      proto_class: "Measurements"
      proto_module: "proto_measurements_pb2"
      required_fields: ["serial_number"]
    device_info:
      proto_class: "DeviceInfo"
      proto_module: "proto_device_info_pb2"
      required_fields: ["imei"]
    config:
      proto_class: "Config"
      proto_module: "proto_config_pb2"
      required_fields: []
```

### 4.2 Another Manufacturer Example

```yaml
type: "coap_protobuf"
config:
  manufacturer: "manufacturer_x"

  device_id_extraction:
    sources:
      - message_type: "device_data"
        field_path: "device.identifier"
        priority: 1

    validation:
      regex: "^[A-Z0-9]{8,16}$"
      normalize: false

  message_types:
    device_data:
      proto_class: "DeviceData"
      proto_module: "device_data_pb2"
      required_fields: ["device"]
```

## 5. Integration with CoAP Connector

```python
# connectors/coap_connector/resources.py
from shared.translation.manager import TranslationManager

class DataRootResource(resource.Resource):
    def __init__(self, kafka_producer, command_consumer):
        super().__init__()
        self.kafka_producer = kafka_producer
        self.command_consumer = command_consumer

        # Load translator configuration (could come from environment or config file)
        translator_config = self._load_translator_config()
        self.translation_manager = TranslationManager([translator_config])

    def _load_translator_config(self):
        """Load translator configuration for the manufacturer."""
        # This could be loaded from environment variables or config files
        return {
            "type": "coap_protobuf",
            "config": {
                "manufacturer": "efento",
                "device_id_extraction": {
                    "sources": [
                        {"message_type": "measurements", "field_path": "serial_number", "priority": 1}
                    ],
                    "validation": {"regex": "^(IMEI:)?[0-9]{15}$", "remove_prefix": "IMEI:"}
                },
                "message_types": {
                    "measurements": {
                        "proto_class": "Measurements",
                        "proto_module": "proto_measurements_pb2",
                        "required_fields": ["serial_number"]
                    }
                }
            }
        }
```

## 6. Preparing Protobuf Files

### 6.1 For Efento Manufacturer

1. **Download Proto Files**: Get the `.proto` files from Efento's GitHub repository

   ```bash
   git clone https://github.com/efento/Proto-files.git
   ```

2. **Compile to Python**: Use `protoc` to generate Python modules

   ```bash
   protoc --python_out=shared/translation/protobuf/proto_schemas/efento/ \
          --proto_path=Proto-files/ \
          Proto-files/proto_measurements.proto \
          Proto-files/proto_device_info.proto \
          Proto-files/proto_config.proto
   ```

3. **Create **init**.py**: Add module initialization

   ```python
   # shared/translation/protobuf/proto_schemas/efento/__init__.py
   """Protobuf schemas for efento"""

   from . import proto_measurements_pb2
   from . import proto_device_info_pb2
   from . import proto_config_pb2
   ```

### 6.2 For Other Manufacturers

Follow the same pattern:

1. Create manufacturer directory under `proto_schemas/`
2. Compile manufacturer's `.proto` files to Python
3. Add appropriate `__init__.py` files

## 7. Benefits of This Design

1. **Single Translator Class**: One ProtobufTranslator handles all manufacturers
2. **Configuration-Driven**: Manufacturer specifics are in config, not code
3. **Extensible**: Add new manufacturers by just adding configuration
4. **Maintainable**: All protobuf logic is centralized
5. **Pre-compiled Schemas**: No runtime compilation needed
6. **Fast Startup**: No downloading or compilation delays
7. **Reliable**: No external dependencies at runtime
8. **Configurable Field Mapping**: No hardcoded field names
9. **Validation Rules**: Manufacturer-specific device ID validation

This design is much cleaner and more maintainable than having separate translator classes for each manufacturer!
