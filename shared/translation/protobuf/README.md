# Protobuf Translation Setup

This directory contains the protobuf translation layer for handling manufacturer-specific device ID extraction.

## Requirements

- Python 3.8+
- protobuf>=4.21.0

## Directory Structure

```
shared/translation/protobuf/
├── proto_schemas/           # Compiled protobuf modules by manufacturer
│   ├── efento/             # Efento protobuf files
│   │   ├── proto_measurements_pb2.py
│   │   ├── proto_device_info_pb2.py
│   │   └── proto_config_pb2.py
│   └── other_manufacturer/ # Other manufacturers...
├── proto_loader.py         # Loads protobuf modules
├── message_parser.py       # Parses protobuf messages
├── device_id_extractor.py  # Extracts device IDs
├── translator.py           # Main ProtobufTranslator class
└── README.md               # This file
```

## Setup Instructions

### 1. Install Dependencies

```bash
pip install protobuf>=4.21.0 PyYAML>=6.0
```

### 2. Compile Protobuf Files

For each manufacturer, you need to compile their `.proto` files to Python modules:

```bash
# Example for Efento
mkdir -p shared/translation/protobuf/proto_schemas/efento
cd shared/translation/protobuf/proto_schemas/efento

# Download proto files from manufacturer repository
# For Efento: https://github.com/efento/Proto-files

# Compile to Python modules
protoc --python_out=. *.proto
```

### 3. Verify Setup

The compiled files should have these names:

- `proto_measurements_pb2.py`
- `proto_device_info_pb2.py`
- `proto_config_pb2.py`

### 4. Configuration

Update `shared/connectors_config.yaml` with manufacturer-specific details:

```yaml
translators:
  - type: "protobuf"
    config:
      manufacturer: "efento"
      device_id_extraction:
        sources:
          - message_type: "measurements"
            field_path: "serialNum"
            priority: 1
      message_types:
        measurements:
          proto_class: "ProtoMeasurements"
          proto_module: "proto_measurements_pb2"
```

## Adding New Manufacturers

1. Create a new directory under `proto_schemas/`
2. Compile the manufacturer's proto files
3. Update the connector configuration
4. The translation layer will automatically load the new protobuf modules

## Troubleshooting

### "No module named 'google'" Error

This indicates the protobuf package isn't installed:

```bash
pip install protobuf>=4.21.0
```

### "Failed to load X for manufacturer" Error

This indicates the proto files aren't compiled or in the wrong location:

1. Verify proto files are compiled to Python modules
2. Check the `proto_module` names match the actual file names
3. Ensure files are in the correct manufacturer directory

### Import Errors

Make sure the Python path includes the shared directory:

```bash
export PYTHONPATH=/app:$PYTHONPATH
```
