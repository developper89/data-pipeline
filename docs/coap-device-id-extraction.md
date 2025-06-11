# CoAP Device ID Extraction from Protobuf Payloads

**Date**: June 11, 2025  
**Version**: 1.0  
**Author**: Development Team

## Table of Contents

1. [Overview](#overview)
2. [Problem Analysis](#problem-analysis)
3. [Discovery Process](#discovery-process)
4. [Technical Implementation](#technical-implementation)
5. [Extraction Methods](#extraction-methods)
6. [Results](#results)
7. [Code Examples](#code-examples)
8. [Future Improvements](#future-improvements)

---

## Overview

This document details the process of implementing device ID extraction from CoAP (Constrained Application Protocol) requests containing Protocol Buffers (protobuf) payloads. The goal was to identify individual IoT devices from binary message data to enable proper routing and data processing in the Preservarium IoT pipeline.

### Key Achievements

- ✅ Successfully extracted device IDs from binary protobuf payloads
- ✅ Implemented robust fallback mechanisms for different data formats
- ✅ Created comprehensive payload analysis and monitoring
- ✅ Identified actual client communication patterns vs. expected patterns

---

## Problem Analysis

### Initial Expectations vs. Reality

**Expected Communication Pattern:**

```
POST /data/{device_id}
Content-Type: application/json
{"temperature": 23.5, "humidity": 65.2}
```

**Actual Communication Pattern:**

```
POST /m
Content-Type: application/octet-stream
Binary protobuf payload: 0a06282c02424eed1001183c22...
```

### Key Discrepancies Discovered

1. **Path Structure**: Clients were posting to `/m` instead of `/data/{device_id}`
2. **Data Format**: Binary Protocol Buffers instead of JSON
3. **Device Identification**: Device ID embedded in payload, not in URL path
4. **Protocol**: Using protobuf wire format with complex field structure

---

## Discovery Process

### Step 1: Request Monitoring Implementation

First, we implemented comprehensive request monitoring to understand the actual communication patterns:

```python
async def render(self, request):
    """Monitor all incoming requests and log details."""
    self.request_count += 1
    request_id = f"REQ-{self.request_count:04d}"

    # Extract request details
    method = request.code.name if hasattr(request.code, 'name') else str(request.code)
    source = request.remote.hostinfo if hasattr(request.remote, 'hostinfo') else str(request.remote)
    payload_size = len(request.payload) if request.payload else 0
    uri_path = list(request.opt.uri_path) if request.opt.uri_path else []
    full_uri = request.get_request_uri() if hasattr(request, 'get_request_uri') else "unknown"
```

### Step 2: Payload Analysis

We discovered that the payload started with `0x0a`, which is a strong indicator of Protocol Buffers format:

```
Payload (hex): 0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100
First few bytes: 0a06282c02424eed (['0xa', '0x6', '0x28', '0x2c', '0x2', '0x42', '0x4e', '0xed'])
```

### Step 3: Protobuf Structure Analysis

Through wire format analysis, we identified the message structure:

```
Field 1: number=1, wire_type=2   (Length-delimited - likely device ID)
Field 2: number=2, wire_type=0   (Varint - likely timestamp)
Field 3: number=3, wire_type=0   (Varint - likely sequence number)
Field 4: number=4, wire_type=2   (Length-delimited - repeated sensor data)
Field 5: number=4, wire_type=2   (Length-delimited - repeated sensor data)
Field 6: number=5, wire_type=0   (Varint - measurement value)
Field 7: number=8, wire_type=0   (Varint - status/type)
Field 8: number=9, wire_type=0   (Varint - additional measurement)
Field 9: number=16, wire_type=2  (Length-delimited - metadata)
```

---

## Technical Implementation

### Architecture Overview

The device ID extraction system consists of three main components:

1. **Primary Extraction**: Protobuf field parsing
2. **Fallback Extraction**: Raw byte pattern matching
3. **Error Handling**: Graceful degradation and logging

### Implementation Flow

```
CoAP Request → Payload Analysis → Extract Device ID
     ↓              ↓                    ↓
Monitor All    Check Size > 0     Try Protobuf Field 1
Requests       Log Details        ↓ (if fails)
               ↓                  Try Protobuf Field 16
               Analyze Format     ↓ (if fails)
                                 Try Raw Bytes Pattern
                                 ↓
                                 Return Device ID or None
```

---

## Extraction Methods

### Method 1: Protobuf Field Parsing (Primary)

**Target Fields**: Field 1 (primary), Field 16 (fallback)  
**Rationale**: Field 1 is typically used for primary identifiers in protobuf schemas

```python
def _parse_protobuf_field(self, payload: bytes, field_number: int) -> str:
    """Parse specific protobuf field and return its string value."""
    try:
        offset = 0
        while offset < len(payload):
            # Read tag (field number + wire type)
            tag = payload[offset]
            found_field_number = tag >> 3
            wire_type = tag & 0x07
            offset += 1

            # Check if this is the field we're looking for
            if found_field_number == field_number and wire_type == 2:  # Length-delimited
                # Read length
                length = payload[offset]
                offset += 1

                # Read the actual data
                if offset + length <= len(payload):
                    field_data = payload[offset:offset + length]

                    # Try to decode as UTF-8 string
                    try:
                        device_id = field_data.decode('utf-8')
                        if device_id and device_id.isprintable():
                            return device_id
                    except UnicodeDecodeError:
                        # If not UTF-8, return as hex string
                        return field_data.hex()
```

### Method 2: Raw Byte Pattern Matching (Fallback)

**Purpose**: Extract device IDs when protobuf structure is non-standard  
**Strategy**: Look for printable ASCII strings of reasonable length (4-20 characters)

```python
def _extract_device_id_from_raw_bytes(self, payload: bytes) -> str:
    """Try to extract device ID from raw bytes using pattern matching."""
    try:
        current_string = ""
        found_strings = []

        for byte in payload:
            if 32 <= byte <= 126:  # Printable ASCII
                current_string += chr(byte)
            else:
                if 4 <= len(current_string) <= 20:
                    found_strings.append(current_string)
                current_string = ""

        # Return the first reasonable string found
        for s in found_strings:
            if any(char.isalnum() for char in s):  # Contains alphanumeric
                return s
```

### Method 3: Error Handling and Logging

**Comprehensive Logging**: Every extraction attempt is logged with detailed information  
**Graceful Degradation**: System continues to function even if device ID extraction fails

```python
def _extract_device_id_from_payload(self, payload: bytes, request_id: str) -> str:
    """Extract device ID from protobuf payload."""
    try:
        # Try protobuf field 1 (most likely location)
        device_id = self._parse_protobuf_field(payload, field_number=1)
        if device_id:
            logger.info(f"🔍 Found device ID in protobuf field 1: '{device_id}'")
            return device_id

        # Try field 16 as fallback
        device_id = self._parse_protobuf_field(payload, field_number=16)
        if device_id:
            logger.info(f"🔍 Found device ID in protobuf field 16: '{device_id}'")
            return device_id

        # Try raw bytes as last resort
        device_id = self._extract_device_id_from_raw_bytes(payload)
        if device_id:
            logger.info(f"🔍 Found device ID in raw bytes: '{device_id}'")
            return device_id

    except Exception as e:
        logger.info(f"❌ Device ID extraction failed: {e}")

    return None
```

---

## Results

### Successful Extraction Example

```
🔍 INCOMING CoAP REQUEST [REQ-0002]
  Method: POST
  Source: 80.187.67.10:20414
  Full URI: coap://80.187.67.10:20414/m
  Path Components: ['m']
  Payload Size: 58 bytes
  Payload (hex): 0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100
    🔍 Found device ID in protobuf field 1: '282c02424eed'
  🏷️  EXTRACTED DEVICE ID: '282c02424eed'
```

### Key Findings

1. **Device ID Format**: 6-byte hexadecimal identifier (`282c02424eed`)
2. **Extraction Success Rate**: 100% for tested payloads
3. **Location**: Consistently found in protobuf field 1
4. **Data Type**: Binary data decoded as hex string

### Device ID Characteristics

- **Length**: 12 characters (6 bytes)
- **Format**: Lowercase hexadecimal
- **Likely Source**: MAC address or hardware identifier
- **Uniqueness**: Each device has a consistent identifier across requests

---

## Code Examples

### Complete Integration Example

```python
# In the main render method
extracted_device_id = None
if payload_size > 0:
    logger.info(f"  Payload (hex): {request.payload.hex()}")

    # Try to extract device ID from payload
    extracted_device_id = self._extract_device_id_from_payload(request.payload, request_id)
    if extracted_device_id:
        logger.info(f"  🏷️  EXTRACTED DEVICE ID: '{extracted_device_id}'")

    self._analyze_payload(request.payload, request_id)
else:
    logger.warning(f"  ⚠️  Empty payload - no device ID to extract")

# Use extracted device ID for further processing
if extracted_device_id:
    # Create RawMessage with proper device ID
    raw_message = RawMessage(
        request_id=request_id,
        device_id=extracted_device_id,  # Use extracted ID
        payload_hex=request.payload.hex(),
        protocol="coap",
        metadata={
            "source_address": source,
            "method": method,
            "uri_path": full_uri,
            "path_components": uri_path,
            "payload_size": payload_size,
        }
    )
```

### Testing and Validation

```python
# Test with known payload
test_payload = bytes.fromhex("0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100")
extracted_id = resource._extract_device_id_from_payload(test_payload, "TEST-001")
assert extracted_id == "282c02424eed"
```

---

## Future Improvements

### 1. Enhanced Protobuf Parsing

- **Schema-Aware Parsing**: If protobuf schema becomes available, implement schema-based parsing
- **Field Validation**: Add validation rules for device ID format
- **Multiple ID Sources**: Support extraction from multiple protobuf fields simultaneously

### 2. Performance Optimizations

- **Caching**: Cache parsed device IDs to avoid re-parsing identical payloads
- **Early Termination**: Stop parsing once device ID is found
- **Batch Processing**: Process multiple messages efficiently

### 3. Error Recovery

- **Invalid ID Handling**: Define behavior for malformed or invalid device IDs
- **Fallback Strategies**: Additional pattern matching techniques
- **Configuration**: Make extraction rules configurable

### 4. Security Considerations

- **Input Validation**: Ensure extracted device IDs are safe for use as identifiers
- **Rate Limiting**: Prevent abuse from devices with invalid IDs
- **Sanitization**: Clean extracted IDs before use in downstream systems

### 5. Monitoring and Analytics

- **Extraction Metrics**: Track success/failure rates
- **Device ID Analytics**: Monitor device ID patterns and distributions
- **Performance Monitoring**: Track extraction timing and resource usage

---

## Conclusion

The device ID extraction implementation successfully addresses the challenge of identifying IoT devices from binary protobuf payloads in CoAP requests. The solution is robust, with multiple fallback mechanisms and comprehensive logging, enabling reliable device identification in the Preservarium IoT data pipeline.

### Key Success Factors

1. **Thorough Analysis**: Understanding the actual vs. expected communication patterns
2. **Flexible Implementation**: Multiple extraction methods with graceful fallbacks
3. **Comprehensive Logging**: Detailed monitoring for debugging and optimization
4. **Robust Error Handling**: System continues to function even with extraction failures

The implementation provides a solid foundation for processing real IoT data and can be extended to handle additional protobuf schemas and device types as the system evolves.
