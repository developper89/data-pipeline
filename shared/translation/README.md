# Translation Layer

A pluggable architecture for extracting device IDs from manufacturer-specific payload formats.

## Overview

The translation layer provides a way to extract device IDs from various payload formats (protobuf, custom binary, etc.) without hardcoding manufacturer-specific logic in the connectors.

## Architecture

```
shared/translation/
├── base.py                    # BaseTranslator interface
├── manager.py                 # TranslationManager
├── protobuf/                  # Protobuf-specific implementations
│   ├── translator.py          # Generic ProtobufTranslator
│   ├── proto_loader.py        # Loads pre-compiled protobuf modules
│   ├── message_parser.py      # Generic protobuf message parsing
│   ├── device_id_extractor.py # Configurable device ID extraction
│   └── proto_schemas/         # Pre-compiled protobuf modules
│       └── efento/            # Efento-specific protobuf modules
└── examples/                  # Usage examples
```

## Quick Start

### 1. Set Up Efento Protobuf Files

Download and compile Efento's protobuf schemas:

```bash
# Download proto files
git clone https://github.com/efento/Proto-files.git

# Create efento schema directory
mkdir -p shared/translation/protobuf/proto_schemas/efento

# Compile protobuf files
protoc --python_out=shared/translation/protobuf/proto_schemas/efento/ \
       --proto_path=Proto-files/ \
       Proto-files/proto_measurements.proto \
       Proto-files/proto_device_info.proto \
       Proto-files/proto_config.proto
```

### 2. Use the Translation Layer

```python
from shared.translation import ProtobufTranslator, TranslationManager
from shared.models.translation import RawData

# Configure Efento translator
efento_config = {
    "manufacturer": "efento",
    "device_id_extraction": {
        "sources": [
            {"message_type": "measurements", "field_path": "serialNum", "priority": 1}
        ],
        "validation": {"regex": r"^[a-f0-9]{16}$"}
    },
    "message_types": {
        "measurements": {
            "proto_class": "ProtoMeasurements",
            "proto_module": "proto_measurements_pb2",
            "required_fields": ["serialNum"]
        }
    }
}

# Create translator and manager
translator = ProtobufTranslator(efento_config)
manager = TranslationManager([translator])

# Extract device ID from payload
raw_data = RawData(payload_bytes=payload_bytes, protocol="coap")
result = manager.extract_device_id(raw_data)

if result.success:
    print(f"Device ID: {result.device_id}")
```

## Configuration

### Protobuf Translator Configuration

The ProtobufTranslator uses YAML-like configuration to handle different manufacturers:

```python
config = {
    "manufacturer": "efento",

    # Device ID extraction rules
    "device_id_extraction": {
        "sources": [
            {
                "message_type": "measurements",
                "field_path": "serialNum",        # Field path in protobuf message
                "priority": 1                     # Lower = higher priority
            }
        ],
        "validation": {
            "regex": r"^[a-f0-9]{16}$",          # Validation regex
            "normalize": False,                   # Whether to normalize (lowercase)
            "remove_prefix": "IMEI:"             # Prefix to remove
        }
    },

    # Message type definitions
    "message_types": {
        "measurements": {
            "proto_class": "ProtoMeasurements",   # Protobuf class name
            "proto_module": "proto_measurements_pb2",  # Python module name
            "required_fields": ["serialNum"]      # Required fields for validation
        }
    }
}
```

## Adding New Manufacturers

1. **Create manufacturer directory**:

   ```bash
   mkdir shared/translation/protobuf/proto_schemas/manufacturer_x
   ```

2. **Compile protobuf files**:

   ```bash
   protoc --python_out=shared/translation/protobuf/proto_schemas/manufacturer_x/ \
          --proto_path=proto_files/ \
          proto_files/device_data.proto
   ```

3. **Create configuration**:

   ```python
   manufacturer_x_config = {
       "manufacturer": "manufacturer_x",
       "device_id_extraction": {
           "sources": [{"message_type": "device_data", "field_path": "device.id", "priority": 1}]
       },
       "message_types": {
           "device_data": {
               "proto_class": "DeviceData",
               "proto_module": "device_data_pb2",
               "required_fields": ["device"]
           }
       }
   }
   ```

4. **Add to translation manager**:
   ```python
   translator = ProtobufTranslator(manufacturer_x_config)
   manager.add_translator(translator)
   ```

## Integration with CoAP Connector

The translation layer integrates with the CoAP connector to extract device IDs:

```python
# connectors/coap_connector/resources.py
from shared.translation import TranslationManager, ProtobufTranslator

class DataRootResource(resource.Resource):
    def __init__(self, kafka_producer, command_consumer):
        # ... existing initialization ...

        # Load translator configurations
        efento_translator = ProtobufTranslator(self._load_efento_config())
        self.translation_manager = TranslationManager([efento_translator])

    async def render_post(self, request):
        # Create RawData from CoAP request
        raw_data = RawData(
            payload_bytes=request.payload,
            protocol="coap",
            metadata={"remote": request.remote}
        )

        # Extract device ID
        result = self.translation_manager.extract_device_id(raw_data)

        if result.success:
            device_id = result.device_id
            # ... continue with message processing ...
        else:
            # Handle extraction failure
            logger.error(f"Device ID extraction failed: {result.error}")
```

## Benefits

1. **Manufacturer Agnostic**: Single codebase handles multiple manufacturers
2. **Configuration Driven**: Add new manufacturers without code changes
3. **Pre-compiled Schemas**: Fast startup, no runtime compilation
4. **Extensible**: Easy to add new payload formats beyond protobuf
5. **Testable**: Clear interfaces make testing straightforward

## Examples

See `shared/translation/examples/` for complete usage examples.
