# coap_gateway/resources.py
import logging
import base64
import json
from datetime import datetime, timezone
import uuid # To generate request ID if needed

import aiocoap
import aiocoap.resource as resource
from kafka.errors import KafkaError

from shared.models.common import RawMessage # Import shared model
from kafka_producer import KafkaMsgProducer # Import the Kafka producer wrapper
from command_consumer import CommandConsumer

logger = logging.getLogger(__name__)

class DataRootResource(resource.Resource): # Inherit from Site for automatic child handling
    """
    Acts as a factory for DeviceDataHandlerResource based on path.
    Listens on the base path (e.g., /data) and delegates requests
    like /data/device123 to a handler for 'device123'.
    """
    def __init__(self, kafka_producer: KafkaMsgProducer, command_consumer: CommandConsumer):
        super().__init__()
        self.kafka_producer = kafka_producer
        self.command_consumer = command_consumer
        self.request_count = 0
        logger.debug(f"Initialized DataRootResource.")


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
        
        # Log comprehensive request details
        logger.info("=" * 80)
        logger.info(f"🔍 INCOMING CoAP REQUEST [{request_id}]")
        logger.info(f"  Method: {method}")
        logger.info(f"  Source: {source}")
        logger.info(f"  Full URI: {full_uri}")
        logger.info(f"  Path Components: {uri_path}")
        logger.info(f"  Payload Size: {payload_size} bytes")
        
        # Log and analyze payload if present
        extracted_device_id = None
        if payload_size > 0:
            logger.info(f"  Payload (hex): {request.payload.hex()}")
            
            # Try to extract device ID from payload
            extracted_device_id = self._extract_device_id_from_payload(request.payload, request_id)
            if extracted_device_id:
                logger.info(f"  🏷️  EXTRACTED DEVICE ID: '{extracted_device_id}'")
            
            # Extract all protobuf data
            extracted_data = self._extract_all_protobuf_data(request.payload, request_id)
            if extracted_data:
                logger.info(f"  📊 EXTRACTED SENSOR DATA:")
                for key, value in extracted_data.items():
                    logger.info(f"    {key}: {value}")
            
            self._analyze_payload(request.payload, request_id)
            
            try:
                payload_text = request.payload.decode('utf-8', errors='replace')[:100]
                logger.info(f"  Payload Preview (text): {repr(payload_text)}")
            except:
                pass
        else:
            logger.warning(f"  ⚠️  Empty payload - no device ID to extract")
                
        logger.info("=" * 80)
        
        # For now, just return Method Not Allowed for all direct requests
        logger.warning(f"[{request_id}] Request received directly to root. Method Not Allowed.")
        return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED, payload=b"Direct root access not allowed")

    def _analyze_payload(self, payload: bytes, request_id: str):
        """Analyze payload and try to decode it in various formats."""
        logger.info(f"  🔬 PAYLOAD ANALYSIS [{request_id}]:")
        
        # Basic structure analysis
        if len(payload) > 0:
            logger.info(f"    First few bytes: {payload[:8].hex()} ({[hex(b) for b in payload[:8]]})")
            
        # Try to detect format based on structure
        if len(payload) >= 2:
            first_byte = payload[0]
            
            # Check for common patterns
            if first_byte == 0x0a:  # Common protobuf field marker
                logger.info(f"    🔍 Possible Protocol Buffers (protobuf) - starts with 0x0a")
                self._try_protobuf_analysis(payload, request_id)
            elif payload.startswith(b'{'):
                logger.info(f"    🔍 Possible JSON format")
                self._try_json_decode(payload, request_id)
            elif payload.startswith(b'<'):
                logger.info(f"    🔍 Possible XML format")
            else:
                logger.info(f"    🔍 Binary format - analyzing structure...")
                self._analyze_binary_structure(payload, request_id)
                
        # Try base64 decode
        try:
            decoded = base64.b64decode(payload)
            logger.info(f"    ✅ Base64 decode successful: {len(decoded)} bytes")
            logger.info(f"       Decoded: {decoded}...")
            logger.info(f"       Decoded hex: {decoded.hex()}...")
        except:
            logger.info(f"    ❌ Not base64 encoded")
            
    def _try_protobuf_analysis(self, payload: bytes, request_id: str):
        """Try to analyze as protobuf."""
        logger.info(f"    📦 Protobuf Analysis:")
        try:
            # Basic protobuf wire format analysis
            offset = 0
            field_count = 0
            while offset < len(payload) and field_count < 10:  # Limit to prevent infinite loops
                if offset >= len(payload):
                    break
                    
                # Read varint (field number and wire type)
                try:
                    tag = payload[offset]
                    field_number = tag >> 3
                    wire_type = tag & 0x07
                    offset += 1
                    
                    logger.info(f"       Field {field_count+1}: number={field_number}, wire_type={wire_type}")
                    
                    # Skip the value based on wire type
                    if wire_type == 0:  # Varint
                        while offset < len(payload) and payload[offset] & 0x80:
                            offset += 1
                        offset += 1
                    elif wire_type == 1:  # 64-bit
                        offset += 8
                    elif wire_type == 2:  # Length-delimited
                        if offset >= len(payload):
                            break
                        length = payload[offset]
                        offset += 1 + length
                    elif wire_type == 5:  # 32-bit
                        offset += 4
                    else:
                        break  # Unknown wire type
                        
                    field_count += 1
                    
                except IndexError:
                    break
                    
            logger.info(f"       Found {field_count} protobuf fields")
        except Exception as e:
            logger.info(f"       Protobuf analysis failed: {e}")
            
    def _try_json_decode(self, payload: bytes, request_id: str):
        """Try to decode as JSON."""
        try:
            decoded = json.loads(payload.decode('utf-8'))
            logger.info(f"    ✅ JSON decode successful: {decoded}")
        except Exception as e:
            logger.info(f"    ❌ JSON decode failed: {e}")
            
    def _analyze_binary_structure(self, payload: bytes, request_id: str):
        """Analyze binary structure patterns."""
        logger.info(f"    🔢 Binary Structure Analysis:")
        
        # Look for repeating patterns
        if len(payload) >= 4:
            # Check if it looks like length-prefixed data
            potential_length = int.from_bytes(payload[:4], 'big')
            if potential_length == len(payload) - 4:
                logger.info(f"       Possible length-prefixed (big-endian): length={potential_length}")
            
            potential_length = int.from_bytes(payload[:4], 'little')
            if potential_length == len(payload) - 4:
                logger.info(f"       Possible length-prefixed (little-endian): length={potential_length}")
                
        # Entropy analysis (simple)
        unique_bytes = len(set(payload))
        entropy_ratio = unique_bytes / len(payload) if len(payload) > 0 else 0
        logger.info(f"       Entropy: {unique_bytes}/{len(payload)} unique bytes ({entropy_ratio:.2f})")
        
        if entropy_ratio < 0.3:
            logger.info(f"       Low entropy - likely structured/compressed data")
        elif entropy_ratio > 0.8:
            logger.info(f"       High entropy - likely random/encrypted data")
    
    def _extract_device_id_from_payload(self, payload: bytes, request_id: str) -> str:
        """Extract device ID from protobuf payload."""
        try:
            # Try to parse protobuf and extract field 1 (most likely device ID)
            device_id = self._parse_protobuf_field(payload, field_number=1)
            if device_id:
                logger.info(f"    🔍 Found device ID in protobuf field 1: '{device_id}'")
                return device_id
            
            # Try field 16 as fallback (could also contain device metadata)
            device_id = self._parse_protobuf_field(payload, field_number=16)
            if device_id:
                logger.info(f"    🔍 Found device ID in protobuf field 16: '{device_id}'")
                return device_id
                
            # If protobuf parsing fails, try to find patterns in raw bytes
            device_id = self._extract_device_id_from_raw_bytes(payload)
            if device_id:
                logger.info(f"    🔍 Found device ID in raw bytes: '{device_id}'")
                return device_id
                
        except Exception as e:
            logger.info(f"    ❌ Device ID extraction failed: {e}")
        
        return None
    
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
    
    def _extract_device_id_from_raw_bytes(self, payload: bytes) -> str:
        """Try to extract device ID from raw bytes using pattern matching."""
        try:
            # Look for printable ASCII strings of reasonable length (4-20 chars)
            current_string = ""
            found_strings = []
            
            for byte in payload:
                if 32 <= byte <= 126:  # Printable ASCII
                    current_string += chr(byte)
                else:
                    if 4 <= len(current_string) <= 20:
                        found_strings.append(current_string)
                    current_string = ""
            
            # Check the last string
            if 4 <= len(current_string) <= 20:
                found_strings.append(current_string)
            
            # Return the first reasonable string found
            for s in found_strings:
                # Filter out strings that look like device IDs
                if any(char.isalnum() for char in s):  # Contains alphanumeric
                    logger.info(f"    Found potential device ID string: '{s}'")
                    return s
                    
        except Exception as e:
            logger.debug(f"Raw bytes device ID extraction failed: {e}")
        
        return None

    def _extract_all_protobuf_data(self, payload: bytes, request_id: str) -> dict:
        """Extract all protobuf fields and interpret their meaning."""
        extracted_data = {}
        
        try:
            offset = 0
            field_count = 0
            
            while offset < len(payload) and field_count < 20:  # Limit to prevent infinite loops
                if offset >= len(payload):
                    break
                    
                # Read tag (field number + wire type)
                tag = payload[offset]
                field_number = tag >> 3
                wire_type = tag & 0x07
                offset += 1
                
                # Extract field value based on wire type
                field_value = None
                field_description = ""
                
                if wire_type == 0:  # Varint
                    field_value, bytes_consumed = self._read_varint(payload, offset)
                    offset += bytes_consumed
                    field_description = self._interpret_varint_field(field_number, field_value)
                    
                elif wire_type == 1:  # 64-bit
                    if offset + 8 <= len(payload):
                        field_value = int.from_bytes(payload[offset:offset+8], 'little')
                        offset += 8
                        field_description = f"64-bit value: {field_value}"
                    else:
                        break
                        
                elif wire_type == 2:  # Length-delimited
                    if offset >= len(payload):
                        break
                    length = payload[offset]
                    offset += 1
                    
                    if offset + length <= len(payload):
                        field_data = payload[offset:offset + length]
                        field_value, field_description = self._interpret_length_delimited_field(field_number, field_data)
                        offset += length
                    else:
                        break
                        
                elif wire_type == 5:  # 32-bit
                    if offset + 4 <= len(payload):
                        field_value = int.from_bytes(payload[offset:offset+4], 'little')
                        offset += 4
                        field_description = f"32-bit value: {field_value}"
                    else:
                        break
                else:
                    break  # Unknown wire type
                
                # Store the extracted field
                if field_value is not None:
                    field_key = f"field_{field_number}"
                    if field_key in extracted_data:
                        # Handle repeated fields
                        if not isinstance(extracted_data[field_key], list):
                            extracted_data[field_key] = [extracted_data[field_key]]
                        extracted_data[field_key].append({
                            "value": field_value,
                            "description": field_description,
                            "wire_type": wire_type
                        })
                    else:
                        extracted_data[field_key] = {
                            "value": field_value,
                            "description": field_description,
                            "wire_type": wire_type
                        }
                
                field_count += 1
                
        except Exception as e:
            logger.info(f"    ❌ Protobuf data extraction failed: {e}")
        
        return extracted_data
    
    def _read_varint(self, payload: bytes, offset: int) -> tuple:
        """Read a varint from the payload and return (value, bytes_consumed)."""
        value = 0
        shift = 0
        bytes_consumed = 0
        
        while offset + bytes_consumed < len(payload):
            byte = payload[offset + bytes_consumed]
            bytes_consumed += 1
            
            value |= (byte & 0x7F) << shift
            
            if (byte & 0x80) == 0:  # MSB is 0, end of varint
                break
                
            shift += 7
            
            if bytes_consumed > 10:  # Prevent infinite loop
                break
        
        return value, bytes_consumed
    
    def _interpret_varint_field(self, field_number: int, value: int) -> str:
        """Interpret varint fields based on field number and value."""
        if field_number == 2:
            # Likely timestamp (Unix timestamp)
            if value > 1000000000 and value < 2000000000:  # Reasonable timestamp range
                try:
                    timestamp = datetime.fromtimestamp(value, tz=timezone.utc)
                    return f"Timestamp: {timestamp.isoformat()} ({value})"
                except:
                    pass
            return f"Possible timestamp or sequence: {value}"
            
        elif field_number == 3:
            return f"Sequence number or count: {value}"
            
        elif field_number == 5:
            # Could be measurement value (temperature, humidity, etc.)
            if value < 1000:  # Likely direct measurement
                return f"Measurement value: {value}"
            else:  # Likely scaled measurement
                scaled = value / 100.0  # Common scaling factor
                return f"Scaled measurement: {scaled} (raw: {value})"
                
        elif field_number == 8:
            return f"Status or type indicator: {value}"
            
        elif field_number == 9:
            if value < 1000:
                return f"Secondary measurement: {value}"
            else:
                scaled = value / 100.0
                return f"Scaled secondary measurement: {scaled} (raw: {value})"
                
        else:
            return f"Unknown varint: {value}"
    
    def _interpret_length_delimited_field(self, field_number: int, data: bytes) -> tuple:
        """Interpret length-delimited fields and return (value, description)."""
        if field_number == 1:
            # Device ID field
            try:
                device_id = data.decode('utf-8')
                if device_id.isprintable():
                    return device_id, f"Device ID (UTF-8): {device_id}"
            except UnicodeDecodeError:
                pass
            return data.hex(), f"Device ID (hex): {data.hex()}"
            
        elif field_number == 4:
            # Sensor data field (appears multiple times)
            return self._parse_sensor_data(data)
            
        elif field_number == 16:
            # Metadata field
            try:
                metadata = data.decode('utf-8')
                if metadata.isprintable():
                    return metadata, f"Metadata (UTF-8): {metadata}"
            except UnicodeDecodeError:
                pass
            return data.hex(), f"Metadata (hex): {data.hex()}"
            
        else:
            # Generic length-delimited field
            try:
                text = data.decode('utf-8')
                if text.isprintable():
                    return text, f"Text data: {text}"
            except UnicodeDecodeError:
                pass
            return data.hex(), f"Binary data (hex): {data.hex()}"
    
    def _parse_sensor_data(self, data: bytes) -> tuple:
        """Parse sensor data from field 4 (repeated sensor readings)."""
        try:
            # Try to parse as nested protobuf structure
            if len(data) >= 3:
                # Check if it looks like nested protobuf
                if data[0] in [0x08, 0x10, 0x18, 0x20]:  # Common protobuf field tags
                    nested_data = self._parse_nested_protobuf(data)
                    if nested_data:
                        return nested_data, f"Sensor reading: {nested_data}"
            
            # Fallback to raw interpretation
            if len(data) == 1:
                return data[0], f"Single byte sensor value: {data[0]}"
            elif len(data) == 2:
                value = int.from_bytes(data, 'little')
                return value, f"16-bit sensor value: {value}"
            elif len(data) == 4:
                value = int.from_bytes(data, 'little')
                return value, f"32-bit sensor value: {value}"
            else:
                return data.hex(), f"Complex sensor data (hex): {data.hex()}"
                
        except Exception as e:
            return data.hex(), f"Sensor data parsing failed (hex): {data.hex()}"
    
    def _parse_nested_protobuf(self, data: bytes) -> dict:
        """Parse nested protobuf structure within sensor data."""
        try:
            nested_fields = {}
            offset = 0
            
            while offset < len(data):
                if offset >= len(data):
                    break
                    
                tag = data[offset]
                field_number = tag >> 3
                wire_type = tag & 0x07
                offset += 1
                
                if wire_type == 0:  # Varint
                    value, bytes_consumed = self._read_varint(data, offset)
                    offset += bytes_consumed
                    
                    # Interpret common sensor field numbers
                    if field_number == 1:
                        nested_fields["sensor_id"] = value
                    elif field_number == 2:
                        nested_fields["sensor_type"] = value
                    elif field_number == 3:
                        nested_fields["value"] = value
                    elif field_number == 4:
                        nested_fields["timestamp"] = value
                    else:
                        nested_fields[f"field_{field_number}"] = value
                        
                else:
                    break  # Only handle varints in nested structure for now
            
            return nested_fields
            
        except Exception as e:
            return None
