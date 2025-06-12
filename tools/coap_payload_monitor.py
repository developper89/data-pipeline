#!/usr/bin/env python3
"""
CoAP UDP Payload Monitor

Real-time monitoring and analysis of CoAP UDP traffic with protobuf payload parsing.
Captures incoming CoAP messages and extracts IoT sensor data using ProtoMeasurements schema.
"""

import socket
import struct
import time
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class CoAPMessage:
    """Simple CoAP message parser according to RFC 7252."""
    
    def __init__(self, data: bytes):
        self.raw_data = data
        self.version = None
        self.message_type = None
        self.token_length = None
        self.code = None
        self.message_id = None
        self.token = None
        self.payload = b''
        self.parse_error = None
        
        try:
            self._parse_message()
        except Exception as e:
            self.parse_error = str(e)
    
    def _parse_message(self):
        """Parse CoAP message structure."""
        if len(self.raw_data) < 4:
            raise ValueError("Message too short for CoAP header")
        
        # Parse fixed header (4 bytes)
        first_byte = self.raw_data[0]
        self.version = (first_byte >> 6) & 0x03
        self.message_type = (first_byte >> 4) & 0x03
        self.token_length = first_byte & 0x0F
        
        self.code = self.raw_data[1]
        self.message_id = struct.unpack('!H', self.raw_data[2:4])[0]
        
        offset = 4
        
        # Parse token
        if self.token_length > 0:
            if offset + self.token_length <= len(self.raw_data):
                self.token = self.raw_data[offset:offset + self.token_length]
                offset += self.token_length
        
        # Find payload marker (0xFF) or assume remaining is payload
        payload_marker = self.raw_data.find(b'\xFF', offset)
        if payload_marker != -1:
            self.payload = self.raw_data[payload_marker + 1:]
        elif offset < len(self.raw_data):
            # No payload marker found, remaining data might be payload
            self.payload = self.raw_data[offset:]
    
    def get_code_string(self) -> str:
        """Convert CoAP code to human-readable string."""
        if self.code is None:
            return "Unknown"
        
        code_class = (self.code >> 5) & 0x07
        code_detail = self.code & 0x1F
        
        if code_class == 0:
            codes = {0: "Empty", 1: "GET", 2: "POST", 3: "PUT", 4: "DELETE"}
            return codes.get(code_detail, f"0.{code_detail:02d}")
        elif code_class == 2:
            return f"2.{code_detail:02d} Success"
        elif code_class == 4:
            return f"4.{code_detail:02d} Client Error"
        elif code_class == 5:
            return f"5.{code_detail:02d} Server Error"
        else:
            return f"{code_class}.{code_detail:02d}"
    
    def get_type_string(self) -> str:
        """Convert message type to string."""
        types = {0: "CON", 1: "NON", 2: "ACK", 3: "RST"}
        return types.get(self.message_type, f"Type{self.message_type}")


class ProtoMeasurementsParser:
    """Parser for ProtoMeasurements protobuf messages."""
    
    def __init__(self):
        self.measurement_types = {
            1: "Temperature", 2: "Humidity", 3: "Light", 4: "Accelerometer",
            5: "Digital Input", 6: "Pressure", 7: "CO2", 8: "Voltage",
            9: "Current", 10: "Power"
        }
    
    def parse_protobuf_payload(self, payload: bytes) -> Dict[str, Any]:
        """Parse ProtoMeasurements from binary payload."""
        if not payload or len(payload) < 2:
            return {'error': 'Payload too short or empty'}
        
        # Check if it looks like protobuf (starts with field 1 tag 0x0a)
        if payload[0] != 0x0a:
            return {'error': 'Not a ProtoMeasurements payload (missing field 1)'}
        
        result = {
            'device_id': None,
            'battery_status': None,
            'measurement_period': None,
            'channels': [],
            'cloud_token': None,
            'parsed_fields': [],
            'raw_size': len(payload),
            'parse_time': datetime.utcnow().isoformat()
        }
        
        try:
            offset = 0
            while offset < len(payload):
                tag = payload[offset]
                field_number = tag >> 3
                wire_type = tag & 0x07
                offset += 1
                
                result['parsed_fields'].append(field_number)
                
                if field_number == 1 and wire_type == 2:  # serial_num (bytes)
                    length = payload[offset]
                    offset += 1
                    if offset + length <= len(payload):
                        serial_data = payload[offset:offset + length]
                        result['device_id'] = serial_data.hex()
                        offset += length
                
                elif field_number == 2 and wire_type == 0:  # battery_status (bool)
                    value, consumed = self._read_varint(payload, offset)
                    result['battery_status'] = bool(value)
                    offset += consumed
                
                elif field_number == 3 and wire_type == 0:  # measurement_period_base (uint32)
                    value, consumed = self._read_varint(payload, offset)
                    result['measurement_period'] = value
                    offset += consumed
                
                elif field_number == 4 and wire_type == 2:  # channels (repeated ProtoChannel)
                    channel, offset = self._parse_channel(payload, offset)
                    result['channels'].append(channel)
                
                elif field_number == 16 and wire_type == 2:  # cloud_token (string)
                    length = payload[offset]
                    offset += 1
                    if length > 0 and offset + length <= len(payload):
                        token_data = payload[offset:offset + length]
                        try:
                            result['cloud_token'] = token_data.decode('utf-8')
                        except UnicodeDecodeError:
                            result['cloud_token'] = token_data.hex()
                    offset += length
                
                else:
                    # Skip unknown fields
                    offset = self._skip_field(payload, offset, wire_type)
                    if offset >= len(payload):
                        break
            
            return result
            
        except Exception as e:
            result['parse_error'] = str(e)
            return result
    
    def _parse_channel(self, payload: bytes, offset: int) -> Tuple[Dict[str, Any], int]:
        """Parse a ProtoChannel message."""
        length = payload[offset]
        offset += 1
        
        if offset + length > len(payload):
            return {'error': 'Channel data truncated'}, offset + length
        
        channel_data = payload[offset:offset + length]
        
        channel = {
            'type': None,
            'type_name': 'Unknown',
            'timestamp': None,
            'start_point': None,
            'sample_offsets': []
        }
        
        data_offset = 0
        while data_offset < len(channel_data):
            tag = channel_data[data_offset]
            field_number = tag >> 3
            wire_type = tag & 0x07
            data_offset += 1
            
            if field_number == 1 and wire_type == 0:  # type (MeasurementType enum)
                value, consumed = self._read_varint(channel_data, data_offset)
                channel['type'] = value
                channel['type_name'] = self.measurement_types.get(value, f'Unknown({value})')
                data_offset += consumed
            
            elif field_number == 2 and wire_type == 0:  # timestamp (int32)
                value, consumed = self._read_varint(channel_data, data_offset)
                channel['timestamp'] = value
                data_offset += consumed
            
            elif field_number == 4 and wire_type == 0:  # start_point (sint32)
                value, consumed = self._read_varint(channel_data, data_offset)
                channel['start_point'] = self._decode_zigzag(value)
                data_offset += consumed
            
            elif field_number == 5 and wire_type == 2:  # sample_offsets (repeated sint32)
                if data_offset < len(channel_data):
                    offsets_length = channel_data[data_offset]
                    data_offset += 1
                    if data_offset + offsets_length <= len(channel_data):
                        offsets_data = channel_data[data_offset:data_offset + offsets_length]
                        channel['sample_offsets'] = self._parse_packed_sint32(offsets_data)
                        data_offset += offsets_length
            
            else:
                data_offset = self._skip_field(channel_data, data_offset, wire_type)
        
        return channel, offset + length
    
    def _read_varint(self, data: bytes, offset: int) -> Tuple[int, int]:
        """Read varint and return (value, bytes_consumed)."""
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
    
    def _skip_field(self, data: bytes, offset: int, wire_type: int) -> int:
        """Skip field based on wire type."""
        if wire_type == 0:  # Varint
            while offset < len(data) and data[offset] & 0x80:
                offset += 1
            if offset < len(data):
                offset += 1
        elif wire_type == 1:  # 64-bit
            offset += 8
        elif wire_type == 2:  # Length-delimited
            if offset < len(data):
                length = data[offset]
                offset += 1 + length
        elif wire_type == 5:  # 32-bit
            offset += 4
        return offset


class CoAPPayloadMonitor:
    """Main monitoring class for CoAP UDP traffic."""
    
    def __init__(self, port: int = 5683, interface: str = '0.0.0.0'):
        self.port = port
        self.interface = interface
        self.socket = None
        self.parser = ProtoMeasurementsParser()
        self.stats = {
            'total_packets': 0,
            'coap_packets': 0,
            'protobuf_packets': 0,
            'parse_errors': 0,
            'unique_devices': set(),
            'start_time': None
        }
    
    def start_monitoring(self, save_to_file: bool = False, output_file: str = None):
        """Start monitoring CoAP UDP traffic."""
        logger.info(f"Starting CoAP payload monitor on {self.interface}:{self.port}")
        
        try:
            # Create UDP socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.interface, self.port))
            
            logger.info(f"✅ Listening for CoAP traffic on UDP port {self.port}")
            logger.info("🔍 Monitoring for ProtoMeasurements payloads...")
            logger.info("Press Ctrl+C to stop")
            print("=" * 80)
            
            self.stats['start_time'] = datetime.utcnow()
            
            while True:
                try:
                    # Receive UDP packet
                    data, addr = self.socket.recvfrom(4096)
                    self.stats['total_packets'] += 1
                    
                    # Parse as CoAP message
                    coap_msg = CoAPMessage(data)
                    
                    if coap_msg.parse_error:
                        logger.debug(f"CoAP parse error from {addr}: {coap_msg.parse_error}")
                        continue
                    
                    self.stats['coap_packets'] += 1
                    
                    # Process the message
                    self._process_coap_message(coap_msg, addr, save_to_file, output_file)
                    
                except socket.timeout:
                    continue
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Error processing packet: {e}")
                    self.stats['parse_errors'] += 1
        
        except Exception as e:
            logger.error(f"Failed to start monitoring: {e}")
        
        finally:
            self._cleanup()
    
    def _process_coap_message(self, coap_msg: CoAPMessage, addr: tuple, 
                            save_to_file: bool, output_file: str):
        """Process a single CoAP message."""
        
        timestamp = datetime.utcnow().isoformat()
        source_ip, source_port = addr
        
        print(f"\n🔍 CoAP Message [{timestamp}]")
        print(f"   Source: {source_ip}:{source_port}")
        print(f"   Type: {coap_msg.get_type_string()}")
        print(f"   Code: {coap_msg.get_code_string()}")
        print(f"   Message ID: {coap_msg.message_id}")
        print(f"   Payload Size: {len(coap_msg.payload)} bytes")
        
        if len(coap_msg.payload) > 0:
            print(f"   Payload (hex): {coap_msg.payload.hex()}")
            
            # Try to parse as ProtoMeasurements
            parsed_data = self.parser.parse_protobuf_payload(coap_msg.payload)
            
            if 'error' not in parsed_data:
                self.stats['protobuf_packets'] += 1
                self._display_protobuf_data(parsed_data)
                
                # Track unique devices
                if parsed_data.get('device_id'):
                    self.stats['unique_devices'].add(parsed_data['device_id'])
                
                # Save to file if requested
                if save_to_file:
                    self._save_to_file(timestamp, addr, coap_msg, parsed_data, output_file)
            else:
                print(f"   ❌ Protobuf parse error: {parsed_data['error']}")
        
        # Display running stats
        self._display_stats()
        print("-" * 80)
    
    def _display_protobuf_data(self, data: Dict[str, Any]):
        """Display parsed protobuf data in a readable format."""
        print(f"   📱 Device ID: {data.get('device_id', 'Unknown')}")
        print(f"   🔋 Battery: {'OK' if data.get('battery_status') else 'LOW'}")
        print(f"   ⏱️  Period: {data.get('measurement_period', 'Unknown')}s")
        print(f"   📊 Channels: {len(data.get('channels', []))}")
        
        for i, channel in enumerate(data.get('channels', []), 1):
            sensor_type = channel.get('type_name', 'Unknown')
            timestamp = channel.get('timestamp')
            sample_count = len(channel.get('sample_offsets', []))
            
            print(f"      Channel {i}: {sensor_type}")
            if timestamp:
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                print(f"         Time: {dt.isoformat()}")
            if sample_count > 0:
                print(f"         Samples: {sample_count}")
                print(f"         Data: {channel['sample_offsets'][:5]}{'...' if sample_count > 5 else ''}")
    
    def _display_stats(self):
        """Display monitoring statistics."""
        uptime = datetime.utcnow() - self.stats['start_time']
        print(f"   📈 Stats: {self.stats['total_packets']} total, "
              f"{self.stats['coap_packets']} CoAP, "
              f"{self.stats['protobuf_packets']} protobuf, "
              f"{len(self.stats['unique_devices'])} devices, "
              f"uptime: {uptime}")
    
    def _save_to_file(self, timestamp: str, addr: tuple, coap_msg: CoAPMessage, 
                     parsed_data: Dict[str, Any], output_file: str):
        """Save captured data to JSON file."""
        if not output_file:
            output_file = f"coap_capture_{int(time.time())}.json"
        
        record = {
            'timestamp': timestamp,
            'source_ip': addr[0],
            'source_port': addr[1],
            'coap': {
                'version': coap_msg.version,
                'type': coap_msg.get_type_string(),
                'code': coap_msg.get_code_string(),
                'message_id': coap_msg.message_id,
                'token': coap_msg.token.hex() if coap_msg.token else None,
                'payload_size': len(coap_msg.payload)
            },
            'payload_hex': coap_msg.payload.hex(),
            'protobuf_data': parsed_data
        }
        
        try:
            with open(output_file, 'a') as f:
                f.write(json.dumps(record) + '\n')
        except Exception as e:
            logger.error(f"Failed to save to file: {e}")
    
    def _cleanup(self):
        """Clean up resources and display final stats."""
        if self.socket:
            self.socket.close()
        
        print("\n" + "=" * 80)
        print("📊 MONITORING SESSION COMPLETE")
        print("=" * 80)
        print(f"Total Packets: {self.stats['total_packets']}")
        print(f"CoAP Packets: {self.stats['coap_packets']}")
        print(f"Protobuf Packets: {self.stats['protobuf_packets']}")
        print(f"Parse Errors: {self.stats['parse_errors']}")
        print(f"Unique Devices: {len(self.stats['unique_devices'])}")
        
        if self.stats['unique_devices']:
            print("Device IDs found:")
            for device_id in sorted(self.stats['unique_devices']):
                print(f"  - {device_id}")
        
        if self.stats['start_time']:
            uptime = datetime.utcnow() - self.stats['start_time']
            print(f"Session Duration: {uptime}")
        
        print("=" * 80)


def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Monitor CoAP UDP traffic and parse protobuf payloads',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Monitor on default port 5683
  %(prog)s -p 8080                  # Monitor on port 8080
  %(prog)s -s                       # Save captured data to JSON file
  %(prog)s -s -o my_capture.json    # Save to specific file
  %(prog)s -v                       # Enable verbose logging
        """
    )
    parser.add_argument('-p', '--port', type=int, default=5683, 
                       help='UDP port to monitor (default: 5683)')
    parser.add_argument('-i', '--interface', default='0.0.0.0',
                       help='Interface to bind to (default: 0.0.0.0 - all interfaces)')
    parser.add_argument('-s', '--save', action='store_true',
                       help='Save captured data to JSON file')
    parser.add_argument('-o', '--output', type=str,
                       help='Output file name (default: auto-generated timestamp)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Create and start monitor
        monitor = CoAPPayloadMonitor(port=args.port, interface=args.interface)
        monitor.start_monitoring(save_to_file=args.save, output_file=args.output)
    except KeyboardInterrupt:
        print("\n⚡ Monitoring stopped by user")
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")


if __name__ == '__main__':
    main() 