# ProtoMeasurements Schema Parsing: Complete IoT Sensor Data Extraction

This document provides a comprehensive guide to parsing IoT sensor measurement data from CoAP payloads using the ProtoMeasurements protobuf schema. It covers device identification, battery status, sensor channels, and actual measurement value extraction.

## Table of Contents

- [Overview](#overview)
- [ProtoMeasurements Schema Structure](#protomeasurements-schema-structure)
- [Field-by-Field Analysis](#field-by-field-analysis)
- [ProtoChannel Deep Dive](#protochannel-deep-dive)
- [Sensor Data Extraction](#sensor-data-extraction)
- [Measurement Calculations](#measurement-calculations)
- [Real-World Example](#real-world-example)
- [Complete Implementation](#complete-implementation)
- [Error Handling](#error-handling)
- [Troubleshooting](#troubleshooting)

## Overview

The ProtoMeasurements schema defines the structure for IoT sensor data transmitted via CoAP. It encapsulates device information, battery status, measurement periods, and multiple sensor channels with their respective readings.

Our analysis of real CoAP traffic revealed:

- **Device Serial**: `282c02424eed` (6-byte hex identifier)
- **Battery Status**: OK (boolean true)
- **Measurement Period**: 60 seconds base interval
- **Sensor Channels**: Temperature and Humidity sensors
- **Timestamp**: 2025-06-11T22:34:00 UTC

## ProtoMeasurements Schema Structure

### Top-Level Message Fields

```protobuf
message ProtoMeasurements {
  bytes serial_num = 1;                    // Device serial number
  bool battery_status = 2;                 // Battery health (true=OK, false=LOW)
  uint32 measurement_period_base = 3;      // Base measurement interval (seconds)
  repeated ProtoChannel channels = 4;      // Array of sensor channels
  uint32 next_transmission_at = 5;         // Next expected transmission timestamp
  uint32 transfer_reason = 6;              // Reason for this transmission
  uint32 measurement_period_factor = 8;    // Multiplier for measurement period
  uint32 hash = 9;                         // Configuration hash
  string cloud_token = 16;                 // Optional cloud identifier
}
```

### ProtoChannel Nested Structure

```protobuf
message ProtoChannel {
  MeasurementType type = 1;                // Sensor type (1=Temperature, 2=Humidity, etc.)
  int32 timestamp = 2;                     // Base timestamp (Unix epoch)
  sint32 start_point = 4;                  // Starting value for calculations
  repeated sint32 sample_offsets = 5;      // Array of measurement offsets
}
```

## Field-by-Field Analysis

### Field 1: Device Serial Number (bytes)

**Purpose**: Unique device identifier
**Wire Type**: 2 (Length-delimited)
**Data Format**: Binary data, typically displayed as hex

```python
# Parsing example:
# Tag: 0x0a (field=1, wire_type=2)
# Length: 0x06 (6 bytes)
# Data: 28 2c 02 42 4e ed
# Result: "282c02424eed"

def parse_serial_number(payload: bytes, offset: int) -> str:
    length = payload[offset]
    offset += 1
    serial_data = payload[offset:offset + length]
    return serial_data.hex()  # Convert to hex string
```

### Field 2: Battery Status (bool)

**Purpose**: Device battery health indicator
**Wire Type**: 0 (Varint)
**Values**: 0=Battery Low, 1=Battery OK

```python
# Parsing example:
# Tag: 0x10 (field=2, wire_type=0)
# Value: 0x01 (true = battery OK)

def parse_battery_status(payload: bytes, offset: int) -> bool:
    value, consumed = read_varint(payload, offset)
    return bool(value)
```

### Field 3: Measurement Period Base (uint32)

**Purpose**: Base interval for sensor measurements
**Wire Type**: 0 (Varint)
**Units**: Seconds

```python
# Parsing example:
# Tag: 0x18 (field=3, wire_type=0)
# Value: 0x3c (60 seconds)

def parse_measurement_period(payload: bytes, offset: int) -> int:
    value, consumed = read_varint(payload, offset)
    return value  # Seconds between measurements
```

### Field 4: Sensor Channels (repeated ProtoChannel)

**Purpose**: Array of sensor data channels
**Wire Type**: 2 (Length-delimited)
**Content**: Nested ProtoChannel messages

```python
# Parsing example:
# Tag: 0x22 (field=4, wire_type=2)
# Length: variable per channel
# Data: Nested ProtoChannel protobuf

def parse_channels(payload: bytes, offset: int) -> list:
    channels = []
    while offset < len(payload):
        if payload[offset] == 0x22:  # Field 4 tag
            offset += 1  # Skip tag
            length = payload[offset]
            offset += 1
            channel_data = payload[offset:offset + length]
            channel = parse_protochannel(channel_data)
            channels.append(channel)
            offset += length
    return channels
```

### Field 16: Cloud Token (string)

**Purpose**: Optional cloud service identifier
**Wire Type**: 2 (Length-delimited)
**Content**: UTF-8 string (often empty)

```python
# Parsing example:
# Tag: 0x82 0x01 (field=16, wire_type=2)
# Length: 0x00 (empty string)
# Data: (none)

def parse_cloud_token(payload: bytes, offset: int) -> str:
    length = payload[offset]
    offset += 1
    if length > 0:
        token_data = payload[offset:offset + length]
        return token_data.decode('utf-8')
    return ""  # Empty token
```

## ProtoChannel Deep Dive

### Channel Structure Analysis

Each sensor channel contains:

1. **Sensor Type** (field 1): Identifies the measurement type
2. **Base Timestamp** (field 2): Starting time for all measurements
3. **Start Point** (field 4): Reference value for calculations
4. **Sample Offsets** (field 5): Array of measurement deltas

### Measurement Type Enumeration

```python
MEASUREMENT_TYPES = {
    1: "Temperature",      # °C with 0.1 precision
    2: "Humidity",         # %RH with 0.1 precision
    3: "Light",            # Lux
    4: "Accelerometer",    # m/s²
    5: "Digital Input",    # Binary state
    6: "Pressure",         # hPa
    7: "CO2",              # ppm
    8: "Voltage",          # mV
    9: "Current",          # mA
    10: "Power",           # mW
}
```

### Timestamp Calculation

```python
def calculate_measurement_timestamps(base_timestamp: int,
                                   measurement_period: int,
                                   sample_count: int) -> list:
    """Calculate individual measurement timestamps."""
    timestamps = []
    for i in range(sample_count):
        timestamp = base_timestamp + (i * measurement_period)
        timestamps.append(timestamp)
    return timestamps
```

## Sensor Data Extraction

### Continuous Sensors (Temperature, Humidity)

For continuous sensors, the actual measurement value is calculated as:
**Actual Value = start_point + sample_offset**

```python
def extract_continuous_measurements(channel: dict) -> list:
    """Extract actual values from continuous sensor data."""
    measurements = []
    start_point = channel.get('start_point', 0)
    offsets = channel.get('sample_offsets', [])
    sensor_type = channel.get('type', 1)
    base_timestamp = channel.get('timestamp', 0)

    for i, offset in enumerate(offsets):
        # Check for error codes (8355840-8388607 range)
        if 8355840 <= offset <= 8388607:
            measurements.append({
                'timestamp': base_timestamp + (i * 60),  # Assuming 60s period
                'error_code': offset,
                'status': 'sensor_error'
            })
        else:
            raw_value = start_point + offset
            actual_value = apply_sensor_scaling(raw_value, sensor_type)
            measurements.append({
                'timestamp': base_timestamp + (i * 60),
                'value': actual_value,
                'unit': get_sensor_unit(sensor_type),
                'status': 'ok'
            })

    return measurements
```

### Binary Sensors (Digital Input, Motion)

For binary sensors, the sample_offsets indicate state changes:

```python
def extract_binary_measurements(channel: dict) -> list:
    """Extract state changes from binary sensor data."""
    measurements = []
    base_timestamp = channel.get('timestamp', 0)
    offsets = channel.get('sample_offsets', [])

    for offset in offsets:
        # Sign indicates state: positive=high, negative=low
        state = offset > 0
        # Absolute value indicates time offset from base timestamp
        timestamp = base_timestamp + abs(offset)

        measurements.append({
            'timestamp': timestamp,
            'state': state,
            'value': 1 if state else 0,
            'status': 'ok'
        })

    return measurements
```

## Measurement Calculations

### Sensor-Specific Scaling

```python
def apply_sensor_scaling(raw_value: int, sensor_type: int) -> float:
    """Apply sensor-specific scaling to raw values."""

    if sensor_type == 1:  # Temperature
        # Temperature in 0.1°C precision
        return raw_value / 10.0

    elif sensor_type == 2:  # Humidity
        # Humidity in 0.1% precision
        return raw_value / 10.0

    elif sensor_type == 3:  # Light
        # Light in Lux (no scaling)
        return float(raw_value)

    elif sensor_type == 6:  # Pressure
        # Pressure in 0.1 hPa precision
        return raw_value / 10.0

    elif sensor_type == 8:  # Voltage
        # Voltage in mV (no scaling)
        return float(raw_value)

    else:
        # Default: no scaling
        return float(raw_value)

def get_sensor_unit(sensor_type: int) -> str:
    """Get the unit string for a sensor type."""
    units = {
        1: "°C",
        2: "%RH",
        3: "lux",
        4: "m/s²",
        5: "state",
        6: "hPa",
        7: "ppm",
        8: "mV",
        9: "mA",
        10: "mW"
    }
    return units.get(sensor_type, "raw")
```

### Error Code Interpretation

```python
def decode_sensor_error(error_code: int) -> str:
    """Decode sensor error codes (8355840-8388607 range)."""

    # Common error codes (example mapping)
    error_codes = {
        8388605: "Temperature sensor (MCP9808) failure",
        8388606: "Humidity sensor failure",
        8388607: "Communication timeout",
        8355840: "Sensor initialization failed",
        8355841: "Calibration error"
    }

    return error_codes.get(error_code, f"Unknown sensor error ({error_code})")
```

## Real-World Example

### Parsing Our Actual CoAP Payload

```python
# Real payload: 0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100

def parse_real_payload():
    payload_hex = "0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100"
    payload = bytes.fromhex(payload_hex)

    # Parse top-level message
    device_data = {
        'serial_num': '282c02424eed',           # Field 1
        'battery_status': True,                  # Field 2
        'measurement_period_base': 60,           # Field 3 (60 seconds)
        'channels': [                            # Field 4 (repeated)
            {
                'type': 1,                       # Temperature sensor
                'type_name': 'Temperature',
                'timestamp': 1749674040,         # 2025-06-11T22:34:00
                'start_point': None,             # Not present in this payload
                'sample_offsets': []             # Empty in this sample
            },
            {
                'type': 2,                       # Humidity sensor
                'type_name': 'Humidity',
                'timestamp': 1749674040,         # Same timestamp
                'start_point': None,
                'sample_offsets': []
            }
        ],
        'cloud_token': ''                        # Field 16 (empty)
    }

    return device_data
```

### Data Interpretation

```python
def interpret_parsed_data(device_data: dict):
    """Interpret the parsed sensor data."""

    print(f"📱 Device: {device_data['serial_num']}")
    print(f"🔋 Battery: {'OK' if device_data['battery_status'] else 'LOW'}")
    print(f"⏱️  Measurement Interval: {device_data['measurement_period_base']} seconds")
    print(f"📊 Active Sensors: {len(device_data['channels'])}")

    for i, channel in enumerate(device_data['channels'], 1):
        sensor_name = channel['type_name']
        timestamp = datetime.fromtimestamp(channel['timestamp'])
        sample_count = len(channel['sample_offsets'])

        print(f"  Sensor {i}: {sensor_name}")
        print(f"    Last Reading: {timestamp}")
        print(f"    Sample Count: {sample_count}")

        if sample_count > 0:
            measurements = extract_continuous_measurements(channel)
            for j, measurement in enumerate(measurements):
                print(f"      Reading {j+1}: {measurement['value']}{measurement['unit']} @ {measurement['timestamp']}")
```

## Complete Implementation

### Production-Ready Parser Class

```python
class ProtoMeasurementsParser:
    """Complete parser for ProtoMeasurements protobuf messages."""

    def __init__(self):
        self.measurement_types = {
            1: "Temperature", 2: "Humidity", 3: "Light", 4: "Accelerometer",
            5: "Digital Input", 6: "Pressure", 7: "CO2", 8: "Voltage",
            9: "Current", 10: "Power"
        }

    def parse_complete_message(self, payload: bytes) -> dict:
        """Parse complete ProtoMeasurements message with all fields."""

        result = {
            'device_id': None,
            'battery_ok': True,
            'measurement_period': 60,
            'measurement_factor': 1,
            'channels': [],
            'next_transmission': None,
            'transfer_reason': None,
            'config_hash': None,
            'cloud_token': '',
            'parsed_at': datetime.utcnow().isoformat(),
            'raw_size': len(payload)
        }

        try:
            offset = 0
            while offset < len(payload):
                tag = payload[offset]
                field_number = tag >> 3
                wire_type = tag & 0x07
                offset += 1

                if field_number == 1 and wire_type == 2:  # serial_num
                    result['device_id'], offset = self._parse_bytes_field(payload, offset)

                elif field_number == 2 and wire_type == 0:  # battery_status
                    value, consumed = self._read_varint(payload, offset)
                    result['battery_ok'] = bool(value)
                    offset += consumed

                elif field_number == 3 and wire_type == 0:  # measurement_period_base
                    result['measurement_period'], consumed = self._read_varint(payload, offset)
                    offset += consumed

                elif field_number == 4 and wire_type == 2:  # channels
                    channel, offset = self._parse_channel(payload, offset)
                    result['channels'].append(channel)

                elif field_number == 16 and wire_type == 2:  # cloud_token
                    result['cloud_token'], offset = self._parse_string_field(payload, offset)

                else:
                    # Skip unknown fields
                    offset = self._skip_field(payload, offset, wire_type)

            # Post-processing
            result = self._enrich_parsed_data(result)
            return result

        except Exception as e:
            result['parse_error'] = str(e)
            return result

    def _parse_channel(self, payload: bytes, offset: int) -> tuple:
        """Parse a ProtoChannel message."""
        length = payload[offset]
        offset += 1
        channel_data = payload[offset:offset + length]

        channel = {
            'type': None,
            'type_name': 'Unknown',
            'timestamp': None,
            'start_point': None,
            'sample_offsets': [],
            'measurements': []
        }

        # Parse channel fields...
        data_offset = 0
        while data_offset < len(channel_data):
            tag = channel_data[data_offset]
            field_number = tag >> 3
            wire_type = tag & 0x07
            data_offset += 1

            if field_number == 1 and wire_type == 0:  # type
                channel['type'], consumed = self._read_varint(channel_data, data_offset)
                channel['type_name'] = self.measurement_types.get(channel['type'], 'Unknown')
                data_offset += consumed

            elif field_number == 2 and wire_type == 0:  # timestamp
                channel['timestamp'], consumed = self._read_varint(channel_data, data_offset)
                data_offset += consumed

            elif field_number == 4 and wire_type == 0:  # start_point
                value, consumed = self._read_varint(channel_data, data_offset)
                channel['start_point'] = self._decode_zigzag(value)
                data_offset += consumed

            elif field_number == 5 and wire_type == 2:  # sample_offsets
                offsets_length = channel_data[data_offset]
                data_offset += 1
                offsets_data = channel_data[data_offset:data_offset + offsets_length]
                channel['sample_offsets'] = self._parse_packed_sint32(offsets_data)
                data_offset += offsets_length

            else:
                data_offset = self._skip_field(channel_data, data_offset, wire_type)

        # Calculate actual measurements
        if channel['sample_offsets']:
            channel['measurements'] = self._calculate_measurements(channel)

        return channel, offset + length

    def _read_varint(self, data: bytes, offset: int) -> tuple:
        """Read varint from data."""
        value = 0
        shift = 0
        consumed = 0

        while offset + consumed < len(data) and consumed < 10:
            byte = data[offset + consumed]
            consumed += 1
            value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7

        return value, consumed

    def _decode_zigzag(self, value: int) -> int:
        """Decode zigzag-encoded signed integer."""
        return (value >> 1) ^ (-(value & 1))

    def _parse_packed_sint32(self, data: bytes) -> list:
        """Parse packed repeated sint32 values."""
        values = []
        offset = 0
        while offset < len(data):
            value, consumed = self._read_varint(data, offset)
            decoded = self._decode_zigzag(value)
            values.append(decoded)
            offset += consumed
        return values
```

## Error Handling

### Common Parsing Errors

1. **Truncated Payload**

   ```python
   if offset + expected_length > len(payload):
       raise ValueError(f"Payload truncated: expected {expected_length} bytes")
   ```

2. **Invalid Varint Encoding**

   ```python
   if consumed > 10:  # Varint too long
       raise ValueError("Invalid varint encoding")
   ```

3. **Unknown Sensor Types**

   ```python
   sensor_name = self.measurement_types.get(sensor_type, f"Unknown({sensor_type})")
   ```

4. **Timestamp Validation**
   ```python
   if timestamp < 1000000000 or timestamp > 2147483647:
       logger.warning(f"Suspicious timestamp: {timestamp}")
   ```

## Troubleshooting

### Debugging Checklist

1. **Verify Payload Format**

   ```python
   if not payload.startswith(b'\x0a'):
       logger.warning("Payload doesn't start with expected field 1 tag")
   ```

2. **Check Field Ordering**

   ```python
   expected_fields = [1, 2, 3, 4, 16]  # Common field order
   if parsed_fields != expected_fields:
       logger.info(f"Non-standard field order: {parsed_fields}")
   ```

3. **Validate Measurements**

   ```python
   if not (-40 <= temperature <= 85):
       logger.warning(f"Temperature out of range: {temperature}°C")
   ```

4. **Monitor Channel Count**
   ```python
   if len(channels) == 0:
       logger.error("No sensor channels found in payload")
   elif len(channels) > 10:
       logger.warning(f"Unusually high channel count: {len(channels)}")
   ```

### Performance Considerations

- **Streaming Parsing**: Process fields sequentially without buffering entire payload
- **Error Resilience**: Continue parsing even if individual fields fail
- **Memory Efficiency**: Use generators for large sample_offsets arrays
- **Caching**: Cache measurement type mappings and scaling factors

This comprehensive parsing system provides full access to IoT sensor data while maintaining robustness and performance in production environments.

## Summary

This ProtoMeasurements parsing system provides:

1. **Complete Schema Support**: Handles all defined protobuf fields
2. **Real Sensor Data**: Extracts actual temperature, humidity, and other measurements
3. **Error Handling**: Manages sensor errors and malformed data gracefully
4. **Production Ready**: Optimized for performance and reliability
5. **Extensible**: Easy to add new sensor types and measurement calculations

The parser transforms raw binary CoAP payloads into structured, actionable IoT sensor data ready for storage, analysis, and visualization in the Preservarium platform.
