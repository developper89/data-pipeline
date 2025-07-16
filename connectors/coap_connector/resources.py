# coap_gateway/resources.py
import logging
from datetime import datetime, timezone
from typing import Optional

import aiocoap
import aiocoap.resource as resource

from shared.models.common import RawMessage
from shared.models.translation import RawData, TranslationResult
from shared.translation.manager import TranslationManager
from shared.translation.factory import TranslatorFactory
from shared.config_loader import get_translator_configs, validate_translator_config
from kafka_producer import KafkaMsgProducer
from command_consumer import CommandConsumer
import config

logger = logging.getLogger(__name__)

class DataRootResource(resource.Resource):
    """CoAP data ingestion resource with translation support."""
    
    def __init__(self, kafka_producer: KafkaMsgProducer, command_consumer: CommandConsumer):
        super().__init__()
        self.kafka_producer = kafka_producer
        self.command_consumer = command_consumer
        self.request_count = 0
        self.path_mapping = {}
        self.translation_manager = self._initialize_translation_manager()
        logger.debug("DataRootResource initialized")

    def _initialize_translation_manager(self) -> TranslationManager:
        """Initialize translation manager from YAML configuration."""
        try:
            translator_configs = get_translator_configs('coap-connector')
            if not translator_configs:
                logger.info("No translator configurations found")
                return TranslationManager([])
            
            translators = []
            for config_item in translator_configs:
                if not validate_translator_config(config_item):
                    logger.warning(f"Invalid translator config: {config_item}")
                    continue
                
                # Extract path mapping
                config_section = config_item.get('config', {})
                if 'path_mapping' in config_section:
                    self.path_mapping = config_section['path_mapping']
                    logger.debug(f"Loaded path mapping: {self.path_mapping}")
                
                # Create translator
                translator = TranslatorFactory.create_translator(config_item)
                if translator:
                    translators.append(translator)
                    logger.debug(f"Created {config_item.get('type')} translator")
                else:
                    logger.warning(f"Failed to create translator: {config_item.get('type')}")
            
            logger.debug(f"Translation manager initialized with {len(translators)} translators")
            return TranslationManager(translators)
            
        except Exception as e:
            logger.error(f"Failed to initialize translation manager: {e}")
            return TranslationManager([])
    
    def _generate_well_known_core_response(self) -> str:
        """Generate CoAP Link Format response for .well-known/core discovery."""
        base_path = "/" + "/".join(config.COAP_BASE_DATA_PATH) if config.COAP_BASE_DATA_PATH else ""
        resources = []
        
        # Add base data endpoint
        if base_path:
            resources.append(f'<{base_path}>;ct=0;rt="iot.data";title="IoT Data Ingestion Endpoint"')
        
        # Add path mapping endpoints
        if self.path_mapping:
            for short_path, description in self.path_mapping.items():
                full_path = f"{base_path}/{short_path}" if base_path else f"/{short_path}"
                resource_type = f"iot.{description.replace('_', '.')}"
                title = description.replace('_', ' ').title()
                resources.append(f'<{full_path}>;ct=0;rt="{resource_type}";title="{title} Endpoint"')
        else:
            # Fallback endpoint
            fallback_path = f"{base_path}/{{device_id}}" if base_path else "/{{device_id}}"
            resources.append(f'<{fallback_path}>;ct=0;rt="iot.device";title="Device Data Endpoint"')
        
        return ",".join(resources)

    def _extract_request_info(self, request):
        """Extract and return request information for logging."""
        method = request.code.name if hasattr(request.code, 'name') else str(request.code)
        source = request.remote.hostinfo if hasattr(request.remote, 'hostinfo') else str(request.remote)
        uri_path_list = list(request.opt.uri_path) if request.opt.uri_path else []
        payload_size = len(request.payload) if request.payload else 0
        
        return {
            'method': method,
            'source': source,
            'uri_path_list': uri_path_list,
            'payload_size': payload_size,
            'uri_path': "/".join(uri_path_list)
        }

    def _log_request(self, request_id: str, request_info: dict):
        """Log request information at appropriate levels."""
        logger.info(f"[{request_id}] {request_info['method']} from {request_info['source']}, "
                   f"payload: {request_info['payload_size']}B, path: {request_info['uri_path']}")
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("=" * 80)
            logger.debug(f"🔍 DETAILED REQUEST [{request_id}]")
            logger.debug(f"  Method: {request_info['method']}")
            logger.debug(f"  Source: {request_info['source']}")
            logger.debug(f"  Path: {request_info['uri_path_list']}")
            logger.debug(f"  Payload: {request_info['payload_size']} bytes")
            logger.debug("=" * 80)

    async def _handle_well_known_core(self, request_id: str, method: str):
        """Handle .well-known/core discovery requests."""
        if method != "GET":
            logger.warning(f"[{request_id}] Method {method} not allowed for .well-known/core")
            return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED)
        
        logger.info(f"[{request_id}] CoAP resource discovery")
        response_body = self._generate_well_known_core_response()
        logger.debug(f"[{request_id}] Discovery response: {response_body}")
        
        return aiocoap.Message(
            code=aiocoap.Code.CONTENT,
            payload=response_body.encode('utf-8'),
            content_format=40  # application/link-format
        )

    def _extract_device_id_using_translation(self, request, request_id: str) -> TranslationResult:
        """Extract device ID using the translation layer."""
        try:
            request_info = self._extract_request_info(request)
            
            raw_data = RawData(
                protocol="coap",
                payload_bytes=request.payload,
                path=request_info['uri_path'],
                metadata={
                    "method": request_info['method'],
                    "source": request_info['source'],
                    "uri_path": request_info['uri_path'],
                    "full_uri": request.get_request_uri() if hasattr(request, 'get_request_uri') else "unknown",
                    "request_id": request_id
                }
            )
            
            result = self.translation_manager.extract_device_id(raw_data)
            
            if result.success:
                logger.debug(f"[{request_id}] Translation successful using {result.translator_used}")
                if hasattr(result, 'parsed_data') and result.parsed_data:
                    logger.debug(f"[{request_id}] Parsed data: {result.parsed_data}")
            else:
                logger.debug(f"[{request_id}] Translation failed: {result.error}")
                    
            return result
            
        except Exception as e:
            error_msg = f"Translation extraction failed: {e}"
            logger.error(f"[{request_id}] {error_msg}")
            return TranslationResult(success=False, error=error_msg)

    async def _handle_pending_commands(self, request_id: str, device_id: str, request):
        """Handle pending commands for a device."""
        if not self.command_consumer:
            return None
            
        try:
            formatted_command = await self.command_consumer.get_formatted_command(device_id)
            if not formatted_command:
                return None
                
            # Get command details for logging
            pending_commands = self.command_consumer.get_pending_commands(device_id)
            command_id = pending_commands[0].get("request_id", "unknown") if pending_commands else "unknown"
            
            logger.info(f"[{request_id}] Sending command {command_id} to device {device_id} ({len(formatted_command)}B)")
            
            # Acknowledge the command
            if pending_commands:
                success = self.command_consumer.acknowledge_command(device_id, command_id)
                if success:
                    logger.info(f"[{request_id}] Command {command_id} acknowledged")
                else:
                    logger.warning(f"[{request_id}] Failed to acknowledge command {command_id}")
            
            logger.debug(f"[{request_id}] Command payload: {formatted_command}")
            return aiocoap.Message(
                mtype=aiocoap.ACK,
                code=aiocoap.Code.CREATED,
                token=request.token,
                payload=formatted_command
            )
            
        except Exception as e:
            logger.error(f"[{request_id}] Error handling pending commands: {e}")
            return None

    async def _generate_acknowledgment_response(self, device_id: str, translator=None) -> Optional[bytes]:
        """Generate manufacturer-specific acknowledgment response when no command is present."""
        try:
            if not translator or not hasattr(translator, 'script_module'):
                return None
                
            # Load script module if needed
            if not translator.script_module and hasattr(translator, 'parser_script_path') and translator.parser_script_path:
                try:
                    logger.info(f"Loading script module for translator: {translator.parser_script_path}")
                    await translator.load_script_module_async()
                except Exception as e:
                    logger.warning(f"Failed to load script module for translator: {e}")
                    return None

            if translator.script_module and hasattr(translator.script_module, 'format_response'):
                response_bytes = translator.script_module.format_response(
                    config={},
                    message_parser=translator.message_parser,
                )
                logger.debug(f"Generated acknowledgment response for device {device_id}: {len(response_bytes) if response_bytes else 0}B")
                return response_bytes
            else:
                logger.debug(f"Script module for device {device_id} does not support format_response")
                
        except Exception as e:
            logger.error(f"Error generating acknowledgment response for device {device_id}: {e}")
            
        return None

    async def _create_standard_response(self, request_id: str, device_id: str, translator, request):
        """Create standard acknowledgment response."""
        try:
            ack_response = await self._generate_acknowledgment_response(device_id, translator)
            
            if ack_response:
                logger.info(f"[{request_id}] Sending acknowledgment response to device {device_id} ({len(ack_response)}B)")
                logger.debug(f"[{request_id}] Acknowledgment payload: {ack_response.hex()}")
                return aiocoap.Message(
                    mtype=aiocoap.ACK,
                    code=aiocoap.Code.CREATED,
                    token=request.token,
                    payload=ack_response
                )
            else:
                logger.debug(f"[{request_id}] Using standard CoAP ACK")
                return aiocoap.Message(mtype=aiocoap.ACK, code=aiocoap.Code.CONTENT, token=request.token)
                
        except Exception as e:
            logger.error(f"[{request_id}] Error generating acknowledgment response: {e}")
            return aiocoap.Message(mtype=aiocoap.ACK, code=aiocoap.Code.CONTENT, token=request.token)

    async def render(self, request):
        """Process incoming CoAP requests."""
        self.request_count += 1
        request_id = f"REQ-{self.request_count:04d}"
        
        request_info = self._extract_request_info(request)
        self._log_request(request_id, request_info)
        
        try:
            # Handle .well-known/core discovery
            if (len(request_info['uri_path_list']) == 2 and 
                request_info['uri_path_list'][0] == ".well-known" and 
                request_info['uri_path_list'][1] == "core"):
                return await self._handle_well_known_core(request_id, request_info['method'])
            
            # Handle empty payload
            # if request_info['payload_size'] == 0:
            #     logger.warning(f"[{request_id}] Empty payload - no device ID to extract")
            #     return aiocoap.Message(code=aiocoap.Code.BAD_REQUEST, payload=b"Bad Request - Empty payload")
            
            # Extract device ID using translation
            translation_result = self._extract_device_id_using_translation(request, request_id)
            if not translation_result.success or not translation_result.device_id:
                logger.warning(f"[{request_id}] Translation failed: {translation_result.error}")
                return aiocoap.Message(code=aiocoap.Code.BAD_REQUEST, payload=b"Bad Request - Invalid endpoint or missing device data")
            
            device_id = translation_result.device_id
            logger.info(f"[{request_id}] Device ID: {device_id} ({translation_result.translator_used})")
            
            # Create and publish RawMessage
            raw_message = RawMessage(
                device_id=device_id,
                timestamp=datetime.now(timezone.utc),
                payload_hex=request.payload.hex(),
                protocol="coap",
                metadata={
                    "protocol": "coap",
                    "source": request_info['source'],
                    "method": request_info['method'],
                    "uri_path": request_info['uri_path'],
                    "translator_used": translation_result.translator_used,
                    "manufacturer": translation_result.translator.manufacturer,
                    "request_id": request_id
                }
            )
            
            self.kafka_producer.publish_raw_message(raw_message)
            logger.info(f"[{request_id}] RawMessage published for device {device_id}")
            
            # Check for pending commands
            command_response = await self._handle_pending_commands(request_id, device_id, request)
            if command_response:
                return command_response
            
            # Generate standard acknowledgment response
            return await self._create_standard_response(request_id, device_id, translation_result.translator, request)
            
        except Exception as e:
            logger.error(f"[{request_id}] Error processing request: {e}", exc_info=True)
            return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR, payload=b"Internal Server Error")