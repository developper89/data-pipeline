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
        
        if payload_size == 0:
            logger.warning(f"[{request_id}] Empty payload received for device {self.device_id}. Sending Bad Request.")
            return aiocoap.Message(code=aiocoap.Code.BAD_REQUEST, payload=b"Payload must not be empty")


        # Log and analyze payload if present
        logger.info(f"  Payload (hex): {request.payload.hex()}")
        self._analyze_payload(request.payload, request_id)
        
        try:
            payload_text = request.payload.decode('utf-8', errors='replace')[:100]
            logger.info(f"  Payload Preview (text): {repr(payload_text)}")
        except:
            pass
                
        logger.info("=" * 80)
        logger.warning(f"[{request_id}] Request received directly to root. Method Not Allowed.")
        return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED, payload=b"Direct root access not allowed")
        
        # try:
        #     # 1. Prepare the RawMessage
        #     raw_message = RawMessage(
        #         request_id=request_id, # Include the generated request ID
        #         device_id=self.device_id,
        #         payload_hex=request.payload.hex(),  # Convert binary to hex string
        #         protocol="coap",
        #         metadata={
        #             "source_address": source,
        #             "method": method,
        #             "uri_path": full_uri,
        #             "path_components": uri_path,
        #             "payload_size": payload_size,
        #         }
        #     )

        #     # 2. Publish to Kafka (Synchronous call to the wrapper)
        #     try:
        #         self.kafka_producer.publish_raw_message(raw_message)
        #         # Logging is handled within the wrapper
        #     except (KafkaError, Exception) as publish_err:
        #          # If publishing fails, log the error and return a server error to CoAP client
        #          logger.error(f"[{request_id}] Failed to publish CoAP data to Kafka for device {self.device_id}: {publish_err}")
        #          # Attempt to publish a gateway error event (this might also fail)
        #          try:
        #               self.kafka_producer.publish_error(
        #                    "Kafka Publish Failed", str(publish_err),
        #                    {"device_id": self.device_id, "method": method},
        #                    request_id=request_id
        #               )
        #          except Exception as e_pub_err:
        #               logger.error(f"[{request_id}] Additionally failed to publish gateway error event after Kafka failure: {e_pub_err}")

        #          return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR, payload=b"Failed to forward data internally")

        #     # 3. Check for pending commands for this device
        #     # Try to get a formatted command using the bidirectional parser
        #     formatted_command = await self.command_consumer.get_formatted_command(self.device_id)
            
        #     if formatted_command:
        #         # Get the command details for logging
        #         pending_commands = self.command_consumer.get_pending_commands(self.device_id)
        #         command_id = pending_commands[0].get("request_id", "unknown") if pending_commands else "unknown"
                
        #         logger.info(f"[{request_id}] Sending formatted command {command_id} to device {self.device_id}")
                
        #         # Acknowledge the command was sent (removes from pending)
        #         if pending_commands:
        #             self.command_consumer.acknowledge_command(self.device_id, command_id)
                
        #         # Send the formatted binary command in response
        #         success_code = aiocoap.Code.CHANGED if method == "PUT" else aiocoap.Code.CREATED
        #         return aiocoap.Message(code=success_code, payload=formatted_command)
        #     else:
        #         # No commands pending or no parser available - just send success code
        #         success_code = aiocoap.Code.CHANGED if method == "PUT" else aiocoap.Code.CREATED
        #         logger.debug(f"[{request_id}] Successfully processed data for {self.device_id}. No commands sent.")
        #         return aiocoap.Message(code=success_code)
            
        # except Exception as e:
        #     # Catch any other unexpected errors during RawMessage creation etc.
        #     logger.exception(f"[{request_id}] Unexpected error processing CoAP request for device {self.device_id}: {e}")
        #     # Attempt to publish a gateway error event
        #     try:
        #         self.kafka_producer.publish_error(
        #             "CoAP Processing Error", str(e),
        #             {"device_id": self.device_id, "method": method},
        #             request_id=request_id
        #          )
        #     except Exception as e_pub_err:
        #          logger.error(f"[{request_id}] Additionally failed to publish gateway error event after processing error: {e_pub_err}")

        #     return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR, payload=b"Internal server error during processing")

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
