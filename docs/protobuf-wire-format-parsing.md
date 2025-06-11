# Protobuf Wire Format Parsing: Device ID Extraction

This document explains how we parse binary protobuf payloads to extract device IDs from CoAP requests, focusing on the wire format protocol and exact parsing logic.

## Table of Contents

- [Overview](#overview)
- [Protobuf Wire Format Basics](#protobuf-wire-format-basics)
- [Wire Types Explained](#wire-types-explained)
- [Real-World Example: Device ID Extraction](#real-world-example-device-id-extraction)
- [Step-by-Step Parsing Process](#step-by-step-parsing-process)
- [Code Implementation](#code-implementation)
- [Why Field 1 and Field 16?](#why-field-1-and-field-16)
- [Troubleshooting](#troubleshooting)

## Overview

When IoT devices send data via CoAP, they often use Protocol Buffers (protobuf) to encode their messages in a compact binary format. To extract device IDs from these messages, we need to understand the **protobuf wire format** - the low-level encoding that defines how data is stored in binary.

Our system successfully extracts device IDs like `282c02424eed` from 58-byte binary payloads sent by external IoT devices.

## Protobuf Wire Format Basics

### Tag Byte Structure

Every protobuf field starts with a **tag byte** that encodes two pieces of information:

```
Tag Byte (8 bits): FFFFFTTT
                   │││││└┴┴─── Wire Type (bits 0-2): How to read the data
                   └┴┴┴┴────── Field Number (bits 3-7): Which field this is
```

### Extracting Information from Tag Byte

```python
tag = payload[offset]                # Read the tag byte
field_number = tag >> 3             # Shift right 3 bits to get field number
wire_type = tag & 0x07              # Mask lower 3 bits to get wire type
```

**Example**: Tag byte `0x0a` (decimal 10, binary `00001010`)

- Field number: `00001` = 1
- Wire type: `010` = 2

## Wire Types Explained

Protobuf defines 5 wire types that tell the parser how to read the following data:

| Wire Type | Name                 | Description                     | Data Format                      |
| --------- | -------------------- | ------------------------------- | -------------------------------- |
| **0**     | Varint               | Variable-length integer         | 1-10 bytes, MSB continuation bit |
| **1**     | 64-bit               | Fixed 64-bit number             | Always exactly 8 bytes           |
| **2**     | **Length-delimited** | **String/bytes/nested message** | **Length byte + data**           |
| **3**     | Start group          | Deprecated                      | (Not used)                       |
| **4**     | End group            | Deprecated                      | (Not used)                       |
| **5**     | 32-bit               | Fixed 32-bit number             | Always exactly 4 bytes           |

### Wire Type 2: Length-Delimited (Most Important)

**Wire type 2** is crucial for device ID extraction because strings and byte arrays use this format:

```
Structure: [Tag][Length][Data...]
Example:    0a    06     282c02424eed
           ↑     ↑      ↑
         Field1  6bytes  Actual device ID
         Type2
```

**Protocol**:

1. Read tag byte to identify field and wire type
2. Read next byte as **length** (number of data bytes that follow)
3. Read **exactly that many bytes** as the field data

## Real-World Example: Device ID Extraction

### Actual CoAP Payload Data

From a real IoT device (`80.187.67.10`):

```
Raw Hex: 0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100
Length:  58 bytes
```

### Field 1 Analysis (Device ID)

Let's trace through the first field step by step:

```
Position: [0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [10] ...
Hex:      0a  06  28  2c  02  42  4e  ed  10  01  18  ...
```

## Step-by-Step Parsing Process

### Step 1: Parse Tag Byte (Position 0)

```python
tag = 0x0a  # = 10 in decimal = 00001010 in binary

# Extract field number and wire type using bit operations
field_number = 0x0a >> 3  # Shift right 3 bits: 00001010 → 00001 = 1
wire_type = 0x0a & 0x07   # Mask lower 3 bits: 00001010 & 00000111 = 010 = 2
```

**Result**: Field 1, Wire type 2 (length-delimited)

### Step 2: Read Length Byte (Position 1)

Since wire type = 2, the protocol specifies:

- Next byte = length of data that follows
- Following bytes = actual field data

```python
length = 0x06  # Position 1 = 6 bytes of data will follow
```

### Step 3: Extract Device ID Data (Positions 2-7)

```python
# Read exactly 6 bytes as specified by the length byte
device_id_bytes = [0x28, 0x2c, 0x02, 0x42, 0x4e, 0xed]

# Convert each byte to 2-character hex representation
hex_chars = []
for byte in device_id_bytes:
    hex_chars.append(f"{byte:02x}")

device_id = "".join(hex_chars)  # Result: "282c02424eed"
```

### Step 4: Verification

**Binary to Hex Conversion**:

- `0x28` → `"28"`
- `0x2c` → `"2c"`
- `0x02` → `"02"`
- `0x42` → `"42"`
- `0x4e` → `"4e"`
- `0xed` → `"ed"`

**Final Device ID**: `"282c02424eed"` (12 characters representing 6 bytes)

## Code Implementation

### Primary Extraction Method

```python
def _parse_protobuf_field(self, payload: bytes, field_number: int) -> str:
    """Parse specific protobuf field and return its string value."""
    try:
        offset = 0
        while offset < len(payload):
            if offset >= len(payload):
                break

            # Read tag (field number + wire type)
            tag = payload[offset]
            found_field_number = tag >> 3
            wire_type = tag & 0x07
            offset += 1

            # Check if this is the field we're looking for
            if found_field_number == field_number and wire_type == 2:  # Length-delimited
                if offset >= len(payload):
                    break

                # Read length byte
                length = payload[offset]
                offset += 1

                # Read the actual data
                if offset + length <= len(payload):
                    field_data = payload[offset:offset + length]

                    # Try to decode as UTF-8 string first
                    try:
                        device_id = field_data.decode('utf-8')
                        if device_id and device_id.isprintable():
                            return device_id
                    except UnicodeDecodeError:
                        # If not UTF-8, return as hex string
                        return field_data.hex()

                break

            # Skip this field's value based on wire type
            elif wire_type == 0:  # Varint
                while offset < len(payload) and payload[offset] & 0x80:
                    offset += 1
                offset += 1
            elif wire_type == 1:  # 64-bit
                offset += 8
            elif wire_type == 2:  # Length-delimited (not our target field)
                if offset >= len(payload):
                    break
                length = payload[offset]
                offset += 1 + length
            elif wire_type == 5:  # 32-bit
                offset += 4
            else:
                break  # Unknown wire type

    except Exception as e:
        logger.debug(f"Protobuf field {field_number} parsing failed: {e}")

    return None
```

### Complete Device ID Extraction Logic

```python
def _extract_device_id_from_payload(self, payload: bytes, request_id: str) -> str:
    """Extract device ID from protobuf payload using multiple strategies."""
    try:
        # Strategy 1: Try protobuf field 1 (most common location)
        device_id = self._parse_protobuf_field(payload, field_number=1)
        if device_id:
            logger.info(f"🔍 Found device ID in protobuf field 1: '{device_id}'")
            return device_id

        # Strategy 2: Try field 16 as fallback (metadata field)
        device_id = self._parse_protobuf_field(payload, field_number=16)
        if device_id:
            logger.info(f"🔍 Found device ID in protobuf field 16: '{device_id}'")
            return device_id

        # Strategy 3: Raw byte pattern matching as last resort
        device_id = self._extract_device_id_from_raw_bytes(payload)
        if device_id:
            logger.info(f"🔍 Found device ID in raw bytes: '{device_id}'")
            return device_id

    except Exception as e:
        logger.info(f"❌ Device ID extraction failed: {e}")

    return None
```

## Why Field 1 and Field 16?

### Field 1: Primary Target

- **Convention**: Field 1 typically contains the primary identifier in protobuf schemas
- **Evidence**: Our real CoAP data shows device ID `282c02424eed` in field 1
- **Efficiency**: Fast parsing since it's usually the first field

### Field 16: Fallback Target

- **Metadata**: Higher field numbers often contain device metadata
- **Flexibility**: Some IoT manufacturers might use different field layouts
- **Robustness**: Provides backup extraction method

### The Three-Layer Strategy

```python
# Layer 1: Protobuf field 1 (primary)
device_id = parse_protobuf_field(payload, field_number=1)

# Layer 2: Protobuf field 16 (metadata fallback)
if not device_id:
    device_id = parse_protobuf_field(payload, field_number=16)

# Layer 3: Raw byte pattern matching (last resort)
if not device_id:
    device_id = extract_from_raw_bytes(payload)
```

## Troubleshooting

### Common Issues and Solutions

1. **"Wire type parsing failed"**

   - **Cause**: Corrupted or non-protobuf payload
   - **Solution**: Raw byte pattern matching kicks in automatically

2. **"Field not found"**

   - **Cause**: Device uses different protobuf schema
   - **Solution**: System tries field 16, then raw bytes

3. **"Invalid hex conversion"**

   - **Cause**: Non-UTF8 binary data in device ID field
   - **Solution**: Code returns hex representation instead

4. **"Device ID extraction failed"**
   - **Cause**: Payload doesn't contain recognizable device identifier
   - **Solution**: Log details for manual analysis

### Debugging Techniques

```python
# Enable detailed logging to see parsing steps
logger.info(f"Tag byte: 0x{tag:02x} → field={field_number}, wire_type={wire_type}")
logger.info(f"Length byte: {length} → reading {length} bytes")
logger.info(f"Raw field data: {field_data.hex()}")
logger.info(f"Decoded device ID: '{device_id}'")
```

### Payload Analysis

The system automatically logs comprehensive payload analysis:

```
🔬 PAYLOAD ANALYSIS [REQ-0001]:
  First few bytes: 0a06282c02424eed (['0xa', '0x6', '0x28', '0x2c', '0x2', '0x42', '0x4e', '0xed'])
  🔍 Possible Protocol Buffers (protobuf) - starts with 0x0a
  📦 Protobuf Analysis:
    Field 1: number=1, wire_type=2
    Field 2: number=2, wire_type=0
    Field 3: number=3, wire_type=0
    Found 9 protobuf fields
```

## Performance Considerations

- **Fast parsing**: Field 1 is typically found immediately
- **Memory efficient**: Processes payload sequentially without buffering
- **Error resilient**: Multiple fallback strategies prevent failures
- **Minimal overhead**: Only parses fields needed for device ID extraction

## Security Notes

- **Input validation**: All byte operations include bounds checking
- **Error handling**: Malformed payloads won't crash the system
- **Logging**: All extraction attempts are logged for security auditing
- **No external dependencies**: Uses only standard Python libraries for parsing
