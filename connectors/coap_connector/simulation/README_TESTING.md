# Efento Parser Testing - Container Edition

This directory contains comprehensive testing tools for the Efento bidirectional parser, designed to work within the Docker container environment.

## Files

- `test_efento_parser.py` - Main comprehensive test suite
- `verify_protobuf_setup.py` - Environment verification script
- `efento_bidirectional_parser.py` - The parser being tested
- `README_TESTING.md` - This documentation

## Container Protobuf Loading (Verified Working ✅)

### Verification Results

The protobuf environment has been verified working in the container:

```
🎉 Protobuf setup verification PASSED
✅ protobuf package available: 6.31.1
✅ protoc compiler available: libprotoc 3.21.12
✅ Proto files: 6 (.proto files found)
✅ Python modules: 6 (_pb2.py files compiled)
✅ Direct import working
✅ ProtobufCompiler working
```

### How Protobuf Modules are Loaded

**Simplified Container Approach (Current Implementation):**

1. Add `/app/shared/translation/protobuf/proto_schemas/efento/protobuf` to `sys.path`
2. Direct import: `import proto_measurements_pb2`, `import proto_device_info_pb2`, etc.
3. Use classes directly: `proto_measurements_pb2.ProtoMeasurements()`

**Production Translator Approach (Reference):**

1. **TranslatorFactory.create_translator()** creates a `ProtobufTranslator`
2. **ProtobufTranslator** initializes with manufacturer config ("efento")
3. **ProtobufMessageParser** uses `ProtobufCompiler` for the manufacturer
4. **ProtobufCompiler** handles compilation and loading automatically

### Container Directory Structure

```
/app/
├── shared/
│   └── translation/
│       └── protobuf/
│           └── proto_schemas/
│               └── efento/
│                   ├── proto_measurements.proto        ✅ Found
│                   ├── proto_device_info.proto         ✅ Found
│                   ├── proto_config.proto              ✅ Found
│                   ├── proto_measurement_types.proto   ✅ Found
│                   ├── proto_rule.proto                ✅ Found
│                   ├── proto_cloud_token_config.proto  ✅ Found
│                   └── protobuf/                       ✅ Compiled
│                       ├── proto_measurements_pb2.py   ✅ Working
│                       ├── proto_device_info_pb2.py    ✅ Working
│                       ├── proto_config_pb2.py         ✅ Working
│                       ├── proto_measurement_types_pb2.py ✅ Working
│                       ├── proto_rule_pb2.py           ✅ Working
│                       └── proto_cloud_token_config_pb2.py ✅ Working
└── connectors/
    └── coap_connector/
        ├── test_efento_parser.py     ✅ Updated
        └── verify_protobuf_setup.py  ✅ Working
```

## Usage

### 1. Verify Protobuf Environment (Optional - Already Verified)

```bash
# Inside container
python verify_protobuf_setup.py
```

Expected output:

- ✅ All protobuf components working
- ✅ 6 proto files found
- ✅ 6 compiled Python modules
- ✅ Direct import successful
- ✅ ProtobufCompiler functional

### 2. Run Comprehensive Tests (Main Usage)

```bash
# Inside container
python test_efento_parser.py
```

This will:

- ✅ Import protobuf modules directly (verified working)
- 📊 Analyze real sensor data patterns
- 🧪 Generate realistic test payloads
- 🔧 Test parser with various scenarios
- 🔧 Test command formatting
- 🌐 Create CoAP client test files

## Test Features

### Streamlined Protobuf Loading

The test script now uses the verified working approach:

```python
# Container protobuf path (verified working)
container_protobuf_path = "/app/shared/translation/protobuf/proto_schemas/efento/protobuf"

# Add to sys.path
sys.path.insert(0, container_protobuf_path)

# Direct import (confirmed working)
import proto_measurements_pb2
import proto_device_info_pb2
import proto_config_pb2
```

### Realistic Test Data (Based on Real Sensor Analysis)

- **Device serial**: `282c0242526f` (from actual Efento sensor)
- **Measurement intervals**: 60 seconds (5s base × 12 factor)
- **Error codes**: 8388557 (actual sensor failure code)
- **Active channels**: TEMPERATURE, HUMIDITY
- **Transmission interval**: 120 seconds
- **Battery status**: OK
- **Config hash**: 10

### Generated Test Files

Creates binary CoAP test files and shell scripts:

- `efento_test_1_m.bin` - Realistic measurements payload
- `efento_test_2_m.bin` - Measurements with error conditions
- `efento_test_3_m.bin` - Basic environmental sensors
- `efento_test_4_m.bin` - Mixed sensor types
- `efento_test_5_i.bin` - Realistic device info payload
- `efento_coap_tests.sh` - Complete test script (uses `coap-client-notls`)

**Note**: The generated shell script uses `coap-client-notls` for CoAP communication without TLS/DTLS encryption.

### Manual CoAP Testing Commands

You can also test individual payloads manually:

```bash
# Test realistic measurements payload
coap-client-notls -m POST -f /tmp/efento_test_1_m.bin coap://localhost:5683/m

# Test device info payload
coap-client-notls -m POST -f /tmp/efento_test_5_i.bin coap://localhost:5683/i

# Test with custom CoAP server
coap-client-notls -m POST -f /tmp/efento_test_1_m.bin coap://your-server:5683/m
```

## Configuration Alignment

### CoAP Connector Config (from `shared/connectors_config.yaml`)

```yaml
- connector_id: "coap-connector"
  translators:
    - type: "protobuf"
      config:
        manufacturer: "efento"
        path_mapping:
          "i": "device_info"
          "m": "measurements"
          "c": "config"
        message_types:
          measurements:
            proto_class: "ProtoMeasurements"
            proto_module: "proto_measurements_pb2"
```

### Test Configuration Alignment ✅

The test script perfectly aligns with production:

- ✅ Manufacturer: "efento"
- ✅ Proto modules: `proto_measurements_pb2`, `proto_device_info_pb2`
- ✅ Path mapping: "m" → measurements, "i" → device_info
- ✅ Device ID extraction from `serial_num` field
- ✅ Same protobuf classes and structure

## Expected Test Output

```
🚀 Efento Parser Test Suite (Container Edition)
==================================================

🐳 Container Environment Analysis
==================================================
✅ /app/shared/translation/protobuf/proto_schemas/efento - EXISTS
✅ /app/shared/translation/protobuf/proto_schemas/efento/protobuf - EXISTS
✅ Parser imported successfully
✅ Added protobuf path: /app/shared/translation/protobuf/proto_schemas/efento/protobuf
✅ Protobuf modules imported successfully

🧪 Running Comprehensive Efento Parser Tests
============================================================

🔍 Analyzing Real Sensor Data from Debug Files
============================================================
📱 Device Serial Analysis:
   Base64: KCwCQlJv
   Hex:    282c0242526f
   Bytes:  [40, 44, 2, 66, 82, 111]

📊 Generating Realistic Environmental Sensors (Normal) payload...
  ✅ Generated payload: 85 bytes

🧪 Testing Parser with Generated Payloads
==================================================
📊 Test 1: ['TEMPERATURE', 'HUMIDITY']
  ✅ Parsed successfully: 10 results

🌐 Creating CoAP Test Files...
  ✅ Created 11 test files:
     📄 /tmp/efento_test_1_m.bin
     📄 /tmp/efento_coap_tests.sh

🎉 Test suite completed - ready for comprehensive testing!
```

## Troubleshooting

### Common Issues (Resolved ✅)

1. **Protobuf modules not found** - ✅ RESOLVED

   - Container path verified working
   - All 6 modules compiled and importable

2. **Parser import failed** - ✅ RESOLVED

   - Parser available in coap_connector directory
   - Import path configured correctly

3. **Container environment mismatch** - ✅ RESOLVED
   - All container paths verified and working
   - Volume mounts confirmed functional

### Quick Debug Commands

```bash
# Verify protobuf (should all return ✅)
python verify_protobuf_setup.py

# Test direct import
python -c "
import sys
sys.path.insert(0, '/app/shared/translation/protobuf/proto_schemas/efento/protobuf')
import proto_measurements_pb2
print('✅ Direct import working')
m = proto_measurements_pb2.ProtoMeasurements()
print('✅ Instance creation working')
"

# Run main test suite
python test_efento_parser.py
```

## Integration with CI/CD

For automated testing in containers:

```bash
# 1. Build container (protobuf already set up)
docker build -t coap-connector:test .

# 2. Run verification (optional - already verified)
docker run coap-connector:test python verify_protobuf_setup.py

# 3. Run comprehensive tests (main testing)
docker run coap-connector:test python test_efento_parser.py

# 4. Test with actual CoAP server (using coap-client-notls)
docker run -p 5683:5683/udp coap-connector:test bash /tmp/efento_coap_tests.sh localhost:5683
```

## Summary

✅ **Container protobuf environment verified and working**  
✅ **Test suite simplified and optimized for container**  
✅ **All dependencies confirmed available**  
✅ **Realistic test data based on actual sensor analysis**  
✅ **Production configuration alignment verified**

The testing environment is ready for comprehensive Efento parser testing!
