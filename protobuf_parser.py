#!/usr/bin/env python3
"""
ProtoMeasurements Parser

A comprehensive parser for IoT sensor measurement data from CoAP payloads
using the ProtoMeasurements protobuf schema.

Based on the schema and documentation provided.
"""

from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProtoMeasurementsParser:
    """Complete parser for ProtoMeasurements protobuf messages."""

    def __init__(self):
        self.measurement_types = {
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

        self.sensor_units = {
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

        self.error_codes = {
            8388605: "Temperature sensor (MCP9808) failure",
            8388606: "Humidity sensor failure", 
            8388607: "Communication timeout",
            8355840: "Sensor initialization failed",
            8355841: "Calibration error"
        }

    def parse_complete_message(self, payload: bytes) -> Dict[str, Any]:
        """Parse complete ProtoMeasurements message with all fields."""

        result = {
            'device_id': None,
            'battery_ok': True,
            'measurement_period_base': 60,
            'measurement_period_factor': 1,
            'channels': [],
            'next_transmission_at': None,
            'transfer_reason': None,
            'config_hash': None,
            'cloud_token': '',
            'parsed_at': datetime.now(timezone.utc).isoformat(),
            'raw_size': len(payload),
            'parse_error': None
        }

        try:
            offset = 0
            while offset < len(payload):
                tag = payload[offset]
                field_number = tag >> 3
                wire_type = tag & 0x07
                offset += 1

                if field_number == 1 and wire_type == 2:  # serial_num (bytes)
                    result['device_id'], offset = self._parse_bytes_field(payload, offset)

                elif field_number == 2 and wire_type == 0:  # battery_status (bool)
                    value, consumed = self._read_varint(payload, offset)
                    result['battery_ok'] = bool(value)
                    offset += consumed

                elif field_number == 3 and wire_type == 0:  # measurement_period_base (uint32)
                    result['measurement_period_base'], consumed = self._read_varint(payload, offset)
                    offset += consumed

                elif field_number == 4 and wire_type == 2:  # channels (repeated ProtoChannel)
                    channel, offset = self._parse_channel(payload, offset)
                    result['channels'].append(channel)

                elif field_number == 5 and wire_type == 0:  # next_transmission_at (uint32)
                    result['next_transmission_at'], consumed = self._read_varint(payload, offset)
                    offset += consumed

                elif field_number == 6 and wire_type == 0:  # transfer_reason (uint32)
                    result['transfer_reason'], consumed = self._read_varint(payload, offset)
                    offset += consumed

                elif field_number == 8 and wire_type == 0:  # measurement_period_factor (uint32)
                    result['measurement_period_factor'], consumed = self._read_varint(payload, offset)
                    offset += consumed

                elif field_number == 9 and wire_type == 0:  # hash (uint32)
                    result['config_hash'], consumed = self._read_varint(payload, offset)
                    offset += consumed

                elif field_number == 16 and wire_type == 2:  # cloud_token (string)
                    result['cloud_token'], offset = self._parse_string_field(payload, offset)

                else:
                    # Skip unknown fields
                    offset = self._skip_field(payload, offset, wire_type)

            # Post-processing: enrich with calculated data
            result = self._enrich_parsed_data(result)
            return result

        except Exception as e:
            result['parse_error'] = str(e)
            logger.error(f"Parse error: {e}")
            return result

    def _parse_channel(self, payload: bytes, offset: int) -> Tuple[Dict[str, Any], int]:
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
            'measurements': [],
            'is_binary': False,
            'raw_data_length': length
        }

        # Parse channel fields
        data_offset = 0
        while data_offset < len(channel_data):
            if data_offset >= len(channel_data):
                break
            
            tag = channel_data[data_offset]
            field_number = tag >> 3
            wire_type = tag & 0x07
            data_offset += 1

            if field_number == 1 and wire_type == 0:  # type (MeasurementType)
                channel['type'], consumed = self._read_varint(channel_data, data_offset)
                channel['type_name'] = self.measurement_types.get(channel['type'], f'Unknown({channel["type"]})')
                channel['is_binary'] = channel['type'] == 5  # Digital Input
                data_offset += consumed

            elif field_number == 2 and wire_type == 0:  # timestamp (int32)
                channel['timestamp'], consumed = self._read_varint(channel_data, data_offset)
                data_offset += consumed

            elif field_number == 4 and wire_type == 0:  # start_point (sint32)
                value, consumed = self._read_varint(channel_data, data_offset)
                channel['start_point'] = self._decode_zigzag(value)
                data_offset += consumed

            elif field_number == 5 and wire_type == 2:  # sample_offsets (repeated sint32, packed)
                offsets_length = channel_data[data_offset]
                data_offset += 1
                if offsets_length > 0:
                    offsets_data = channel_data[data_offset:data_offset + offsets_length]
                    channel['sample_offsets'] = self._parse_packed_sint32(offsets_data)
                    data_offset += offsets_length

            else:
                # Skip unknown fields
                data_offset = self._skip_field(channel_data, data_offset, wire_type)

        return channel, offset + length

    def _read_varint(self, data: bytes, offset: int) -> Tuple[int, int]:
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

    def _parse_packed_sint32(self, data: bytes) -> List[int]:
        """Parse packed repeated sint32 values."""
        values = []
        offset = 0
        while offset < len(data):
            value, consumed = self._read_varint(data, offset)
            decoded = self._decode_zigzag(value)
            values.append(decoded)
            offset += consumed
        return values

    def _parse_bytes_field(self, payload: bytes, offset: int) -> Tuple[str, int]:
        """Parse a bytes field and return as hex string."""
        length = payload[offset]
        offset += 1
        if length > 0:
            data = payload[offset:offset + length]
            return data.hex(), offset + length
        return "", offset

    def _parse_string_field(self, payload: bytes, offset: int) -> Tuple[str, int]:
        """Parse a string field."""
        length = payload[offset]
        offset += 1
        if length > 0:
            data = payload[offset:offset + length]
            return data.decode('utf-8'), offset + length
        return "", offset

    def _skip_field(self, payload: bytes, offset: int, wire_type: int) -> int:
        """Skip unknown field based on wire type."""
        if wire_type == 0:  # Varint
            while offset < len(payload) and (payload[offset] & 0x80):
                offset += 1
            return offset + 1 if offset < len(payload) else offset
        elif wire_type == 2:  # Length-delimited
            if offset < len(payload):
                length = payload[offset]
                return offset + 1 + length
            return offset
        else:
            logger.warning(f"Unknown wire type {wire_type}, skipping")
            return offset + 1

    def _enrich_parsed_data(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Add calculated measurements and metadata to parsed data."""
        
        # Calculate actual measurement period
        actual_period = result['measurement_period_base'] * max(1, result['measurement_period_factor'])
        result['actual_measurement_period'] = actual_period

        # Process each channel to extract measurements
        for channel in result['channels']:
            if channel['sample_offsets']:
                if channel['is_binary']:
                    channel['measurements'] = self._extract_binary_measurements(channel, actual_period)
                else:
                    channel['measurements'] = self._extract_continuous_measurements(channel, actual_period)
            
            # Add human-readable timestamp
            if channel['timestamp']:
                channel['timestamp_iso'] = datetime.fromtimestamp(channel['timestamp'], timezone.utc).isoformat()

        # Add device metadata
        if result['next_transmission_at']:
            result['next_transmission_iso'] = datetime.fromtimestamp(
                result['next_transmission_at'], timezone.utc
            ).isoformat()

        if result['transfer_reason'] is not None:
            result['transfer_reason_decoded'] = self._decode_transfer_reason(result['transfer_reason'])

        return result

    def _extract_continuous_measurements(self, channel: Dict[str, Any], period: int) -> List[Dict[str, Any]]:
        """Extract actual values from continuous sensor data."""
        measurements = []
        start_point = channel.get('start_point', 0)
        offsets = channel.get('sample_offsets', [])
        sensor_type = channel.get('type', 1)
        base_timestamp = channel.get('timestamp', 0)

        for i, offset in enumerate(offsets):
            timestamp = base_timestamp + (i * period)
            
            # Check for error codes (8355840-8388607 range)
            if 8355840 <= offset <= 8388607:
                measurements.append({
                    'timestamp': timestamp,
                    'timestamp_iso': datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                    'error_code': offset,
                    'error_description': self.error_codes.get(offset, f"Unknown sensor error ({offset})"),
                    'status': 'sensor_error'
                })
            else:
                raw_value = start_point + offset
                actual_value = self._apply_sensor_scaling(raw_value, sensor_type)
                measurements.append({
                    'timestamp': timestamp,
                    'timestamp_iso': datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                    'value': actual_value,
                    'raw_value': raw_value,
                    'unit': self.sensor_units.get(sensor_type, "raw"),
                    'status': 'ok'
                })

        return measurements

    def _extract_binary_measurements(self, channel: Dict[str, Any], period: int) -> List[Dict[str, Any]]:
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
                'timestamp_iso': datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                'state': state,
                'value': 1 if state else 0,
                'status': 'ok'
            })

        return measurements

    def _apply_sensor_scaling(self, raw_value: int, sensor_type: int) -> float:
        """Apply sensor-specific scaling to raw values."""
        
        if sensor_type == 1:  # Temperature
            return raw_value / 10.0
        elif sensor_type == 2:  # Humidity
            return raw_value / 10.0
        elif sensor_type == 3:  # Light
            return float(raw_value)
        elif sensor_type == 6:  # Pressure
            return raw_value / 10.0
        elif sensor_type == 8:  # Voltage
            return float(raw_value)
        else:
            return float(raw_value)

    def _decode_transfer_reason(self, reason: int) -> Dict[str, Any]:
        """Decode transfer reason bit flags."""
        decoded = {
            'first_message_after_reset': bool(reason & (1 << 0)),
            'user_button_triggered': bool(reason & (1 << 1)),
            'user_ble_triggered': bool(reason & (1 << 2)),
            'retry_count': (reason >> 3) & 0x1F,  # bits 3-7
            'triggered_rules': [],
            'end_of_limit_triggered': bool(reason & (1 << 20))
        }
        
        # Check rules 1-12 (bits 8-19)
        for i in range(12):
            if reason & (1 << (8 + i)):
                decoded['triggered_rules'].append(i + 1)
        
        return decoded

    def print_parsed_data(self, data: Dict[str, Any]) -> None:
        """Print parsed data in a human-readable format."""
        
        print("=" * 60)
        print("PROTOBUF MEASUREMENTS PARSER RESULTS")
        print("=" * 60)
        
        if data.get('parse_error'):
            print(f"❌ Parse Error: {data['parse_error']}")
            return
        
        # Device information
        print(f"📱 Device ID: {data.get('device_id', 'Unknown')}")
        print(f"🔋 Battery Status: {'OK' if data.get('battery_ok') else 'LOW'}")
        print(f"⏱️  Measurement Period: {data.get('measurement_period_base')}s (base) × {data.get('measurement_period_factor')} (factor) = {data.get('actual_measurement_period')}s")
        print(f"📊 Active Channels: {len(data.get('channels', []))}")
        
        if data.get('config_hash'):
            print(f"🔧 Config Hash: {data['config_hash']}")
        
        if data.get('next_transmission_at'):
            print(f"📡 Next Transmission: {data.get('next_transmission_iso', 'Unknown')}")
        
        if data.get('transfer_reason_decoded'):
            reason = data['transfer_reason_decoded']
            print(f"📤 Transfer Reason:")
            if reason['first_message_after_reset']:
                print(f"   • First message after reset")
            if reason['user_button_triggered']:
                print(f"   • User button triggered")
            if reason['retry_count'] > 0:
                print(f"   • Retry count: {reason['retry_count']}")
            if reason['triggered_rules']:
                print(f"   • Rules triggered: {reason['triggered_rules']}")
        
        print(f"🕒 Parsed at: {data.get('parsed_at')}")
        print(f"📏 Payload size: {data.get('raw_size')} bytes")
        
        # Channel details
        print("\n" + "=" * 60)
        print("SENSOR CHANNELS")
        print("=" * 60)
        
        for i, channel in enumerate(data.get('channels', []), 1):
            print(f"\n📡 Channel {i}: {channel.get('type_name', 'Unknown')} (Type {channel.get('type', '?')})")
            
            if channel.get('timestamp_iso'):
                print(f"   🕐 Base Timestamp: {channel['timestamp_iso']}")
            
            if channel.get('start_point') is not None:
                print(f"   🎯 Start Point: {channel['start_point']}")
            
            print(f"   📈 Sample Count: {len(channel.get('sample_offsets', []))}")
            
            # Show measurements
            measurements = channel.get('measurements', [])
            if measurements:
                print(f"   📊 Measurements:")
                for j, measurement in enumerate(measurements[:5]):  # Show first 5
                    if measurement['status'] == 'sensor_error':
                        print(f"      {j+1}. ERROR at {measurement['timestamp_iso']}: {measurement['error_description']}")
                    else:
                        value_str = f"{measurement['value']}{measurement.get('unit', '')}"
                        print(f"      {j+1}. {value_str} at {measurement['timestamp_iso']}")
                
                if len(measurements) > 5:
                    print(f"      ... and {len(measurements) - 5} more measurements")
            else:
                print(f"   📊 No measurements (offsets: {channel.get('sample_offsets', [])})")


def main():
    """Example usage with the documented payload."""
    
    # Example payload from the documentation
    payload_hex = "0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100"
    payload_bytes = bytes.fromhex(payload_hex)
    
    parser = ProtoMeasurementsParser()
    
    print("Parsing example payload from documentation...")
    print(f"Payload hex: {payload_hex}")
    print(f"Payload length: {len(payload_bytes)} bytes\n")
    
    # Parse the payload
    result = parser.parse_complete_message(payload_bytes)
    
    # Print results
    parser.print_parsed_data(result)
    
    # Also return the raw parsed data for programmatic use
    return result


if __name__ == "__main__":
    parsed_data = main() 