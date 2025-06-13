# Protobuf Translation Setup

This directory contains the protobuf translation layer for handling manufacturer-specific device ID extraction with **dynamic compilation**.

## Requirements

- Python 3.8+
- protobuf>=4.21.0
- **protoc (Protocol Buffer Compiler)**

## Installation

### 1. Install Python Dependencies

```bash
pip install protobuf>=4.21.0 PyYAML>=6.0
```

### 2. Install protoc Compiler

#### Ubuntu/Debian:

```bash
apt-get update
apt-get install -y protobuf-compiler
```

#### Alpine Linux (for Docker):

```bash
apk add --no-cache protobuf-dev protobuf
```

#### macOS:

```bash
brew install protobuf
```

#### Verify Installation:

```bash
protoc --version
# Should output: libprotoc 3.x.x or higher
```

## Directory Structure

```
shared/translation/protobuf/
├── proto_schemas/           # Raw .proto files by manufacturer
│   ├── efento/             # Efento .proto files
│   │   ├── proto_measurements.proto
│   │   ├── proto_device_info.proto
│   │   └── proto_config.proto
│   └── other_manufacturer/ # Other manufacturers...
├── proto_compiler.py       # Dynamic protobuf compiler
├── message_parser.py       # Parses protobuf messages
├── device_id_extractor.py  # Extracts device IDs
├── translator.py           # Main ProtobufTranslator class
└── README.md               # This file
```

## How It Works

### **Dynamic Compilation Process:**

1. **Runtime Compilation**: When the ProtobufTranslator initializes:

   - Scans `proto_schemas/{manufacturer}/` for `.proto` files
   - Uses `protoc` to compile them to Python modules in a temporary directory
   - Loads the compiled modules dynamically into memory

2. **Message Processing**: When processing data:

   - Uses compiled protobuf classes to parse binary data
   - Extracts device IDs based on configuration
   - Returns structured data for further processing

3. **Cleanup**: Temporary compiled files are automatically cleaned up

## Setup Instructions

### 1. Add .proto Files

Place your manufacturer's `.proto` files in the appropriate directory:

```bash
# Example for Efento
mkdir -p shared/translation/protobuf/proto_schemas/efento
# Copy .proto files to this directory
cp *.proto shared/translation/protobuf/proto_schemas/efento/
```

### 2. Configuration

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

### 3. No Pre-compilation Needed!

Unlike the old approach, you don't need to manually compile `.proto` files. The system handles compilation automatically at runtime.

## Adding New Manufacturers

1. Create directory: `shared/translation/protobuf/proto_schemas/{manufacturer_name}/`
2. Add `.proto` files to the directory
3. Update connector configuration with manufacturer details
4. The translation layer will automatically compile and load the protobuf modules

## Benefits of Dynamic Compilation

✅ **No Import Issues**: Avoids Python import path problems  
✅ **Automatic Dependencies**: Handles cross-references between .proto files  
✅ **Version Agnostic**: Works with any protoc version  
✅ **Clean Setup**: Just place .proto files, no manual compilation  
✅ **Isolated Compilation**: Each manufacturer gets its own compilation context

## Troubleshooting

### "protoc command not found"

Install the protobuf compiler:

```bash
# Ubuntu/Debian
apt-get install protobuf-compiler

# Alpine (Docker)
apk add protobuf-dev

# macOS
brew install protobuf
```

### "protoc compilation failed"

Check your .proto files for syntax errors:

```bash
protoc --proto_path=/path/to/protos --python_out=/tmp your_file.proto
```

### "No .proto files found"

Ensure .proto files are in the correct directory:

```bash
ls -la shared/translation/protobuf/proto_schemas/efento/
# Should show .proto files
```

### Performance Considerations

- Compilation happens once at translator initialization
- Compiled modules are cached in memory
- Temporary files are cleaned up automatically
- For production, consider pre-warming the cache

## Docker Integration

Add to your Dockerfile:

```dockerfile
# Install protoc compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy proto files
COPY shared/translation/protobuf/proto_schemas/ /app/shared/translation/protobuf/proto_schemas/
```
