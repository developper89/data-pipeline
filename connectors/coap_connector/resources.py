# coap_gateway/resources.py
import logging
from datetime import datetime, timezone

import aiocoap
import aiocoap.resource as resource

from shared.models.common import RawMessage # Import shared model
from shared.models.translation import RawData, TranslationResult
from shared.translation.manager import TranslationManager
from shared.translation.factory import TranslatorFactory
from shared.config_loader import get_translator_configs, validate_translator_config
from kafka_producer import KafkaMsgProducer # Import the Kafka producer wrapper
from command_consumer import CommandConsumer
import config

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
        self.path_mapping = {}  # Store path mapping from config
        
        logger.info("🚀 Initializing DataRootResource...")
        
        # Initialize translation manager
        self.translation_manager = self._initialize_translation_manager()
        
        logger.info(f"✅ DataRootResource initialized with translation manager.")

    def _initialize_translation_manager(self) -> TranslationManager:
        """Initialize the translation manager from YAML configuration."""
        try:
            # Get translator configurations for the CoAP connector
            translator_configs = get_translator_configs('coap-connector')
            
            if not translator_configs:
                logger.info("No translator configurations found - running without translators")
                return TranslationManager([])
            
            # Create translators based on configuration
            translators = []
            for translator_config in translator_configs:
                # Validate configuration
                if not validate_translator_config(translator_config):
                    logger.warning(f"Invalid translator config: {translator_config}")
                    continue
                
                translator_type = translator_config.get('type')
                priority = translator_config.get('priority', 999)
                
                # Extract and store path_mapping if it exists
                config_section = translator_config.get('config', {})
                if 'path_mapping' in config_section:
                    self.path_mapping = config_section['path_mapping']
                    logger.info(f"Loaded path mapping: {self.path_mapping}")
                
                # Use factory to create translator
                translator = TranslatorFactory.create_translator(translator_config)
                if translator:
                    translators.append(translator)
                    logger.info(f"Created {translator_type} translator with priority {priority}")
                else:
                    logger.warning(f"Failed to create translator of type: {translator_type}")
            
            translation_manager = TranslationManager(translators)
            if translators:
                logger.info(f"Translation manager initialized with {len(translators)} translator(s) from YAML config")
            else:
                logger.info("Translation manager initialized with no translators")
            return translation_manager
            
        except Exception as e:
            logger.error(f"Failed to initialize translation manager from config: {e}")
            logger.info("Creating empty translation manager")
            return TranslationManager([])
    
    def _generate_well_known_core_response(self) -> str:
        """Generate CoAP Link Format response for .well-known/core discovery."""
        # Generate the base data path from config
        base_path = "/" + "/".join(config.COAP_BASE_DATA_PATH) if config.COAP_BASE_DATA_PATH else ""
        
        # CoAP Link Format according to RFC 6690
        # Format: </path>;ct=content-type;rt=resource-type;if=interface;title="description"
        resources = []
        
        # Add base data endpoint
        if base_path:
            resources.append(f'<{base_path}>;ct=0;rt="iot.data";title="IoT Data Ingestion Endpoint"')
        
        # Add endpoints from path_mapping configuration
        if self.path_mapping:
            for short_path, description in self.path_mapping.items():
                full_path = f"{base_path}/{short_path}" if base_path else f"/{short_path}"
                # Create resource types based on the description
                resource_type = f"iot.{description.replace('_', '.')}"
                title = description.replace('_', ' ').title()
                resources.append(f'<{full_path}>;ct=0;rt="{resource_type}";title="{title} Endpoint"')
        else:
            # Fallback if no path mapping is configured
            logger.warning("No path mapping configured, using generic endpoint")
            fallback_path = f"{base_path}/{{device_id}}" if base_path else "/{{device_id}}"
            resources.append(f'<{fallback_path}>;ct=0;rt="iot.device";title="Device Data Endpoint"')
        
        return ",".join(resources)

    async def render(self, request):
        """Monitor all incoming requests and log details."""
        self.request_count += 1
        request_id = f"REQ-{self.request_count:04d}"
        
        # Extract basic request info for logging
        method = request.code.name if hasattr(request.code, 'name') else str(request.code)
        source = request.remote.hostinfo if hasattr(request.remote, 'hostinfo') else str(request.remote)
        payload_size = len(request.payload) if request.payload else 0
        uri_path_list = list(request.opt.uri_path) if request.opt.uri_path else []
        
        # Log only essential info at INFO level for performance
        logger.info(f"[{request_id}] {method} from {source}, payload: {payload_size}B, path: {'/'.join(uri_path_list)}")
        
        # Detailed logging only at DEBUG level
        if logger.isEnabledFor(logging.DEBUG):
            uri_path = "/".join(uri_path_list)
            full_uri = request.get_request_uri() if hasattr(request, 'get_request_uri') else "unknown"
            logger.debug("=" * 80)
            logger.debug(f"🔍 DETAILED REQUEST [{request_id}]")
            logger.debug(f"  Method: {method}")
            logger.debug(f"  Source: {source}")
            logger.debug(f"  Full URI: {full_uri}")
            logger.debug(f"  Path Components: {uri_path_list}")
            logger.debug(f"  Payload Size: {payload_size} bytes")
            logger.debug("=" * 80)
        
        try:
            # Handle .well-known/core discovery requests
            if len(uri_path_list) == 2 and uri_path_list[0] == ".well-known" and uri_path_list[1] == "core":
                if method == "GET":
                    logger.info(f"[{request_id}] CoAP resource discovery")
                    well_known_response = self._generate_well_known_core_response()
                    logger.debug(f"[{request_id}] Discovery response: {well_known_response}")
                    
                    return aiocoap.Message(
                        code=aiocoap.Code.CONTENT,
                        payload=well_known_response.encode('utf-8'),
                        content_format=40  # application/link-format
                    )
                else:
                    logger.warning(f"[{request_id}] Method {method} not allowed for .well-known/core")
                    return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED)
            
            # Process payload if present
            translation_result = None
            if payload_size > 0:
                # Use translation manager to extract device ID
                translation_result = self._extract_device_id_using_translation(request, request_id)
                if translation_result and translation_result.success and translation_result.device_id:
                    logger.info(f"[{request_id}] Device ID: {translation_result.device_id} ({translation_result.translator_used})")
                elif translation_result and not translation_result.success:
                    logger.warning(f"[{request_id}] Translation failed: {translation_result.error}")
            else:
                logger.debug(f"[{request_id}] Empty payload - no device ID to extract")
            
            # Create RawMessage if device ID was extracted successfully
            if translation_result and translation_result.success and translation_result.device_id:
                try:
                    # Create RawMessage with extracted device_id
                    raw_message = RawMessage(
                        device_id=translation_result.device_id,
                        timestamp=datetime.now(timezone.utc),
                        payload_hex=request.payload.hex(),
                        protocol="coap",
                        metadata={
                            "source": source,
                            "method": method,
                            "uri_path": "/".join(uri_path_list),
                            "translator_used": translation_result.translator_used,
                            "request_id": request_id
                        }
                    )
                    
                    logger.info(f"[{request_id}] RawMessage created for device {translation_result.device_id}")
                    
                    # Publish to Kafka
                    self.kafka_producer.publish_raw_message(raw_message)
                    
                    # Check for pending commands after publishing the raw message
                    if self.command_consumer:
                        try:
                            formatted_command = await self.command_consumer.get_formatted_command(translation_result.device_id)
                            
                            if formatted_command:
                                # Get the command details for logging
                                pending_commands = self.command_consumer.get_pending_commands(translation_result.device_id)
                                command_id = pending_commands[0].get("request_id", "unknown") if pending_commands else "unknown"
                                
                                logger.info(f"[{request_id}] Sending command {command_id} to device {translation_result.device_id} ({len(formatted_command)}B)")
                                
                                # Acknowledge the command was sent (removes from pending)
                                if pending_commands:
                                    success = self.command_consumer.acknowledge_command(translation_result.device_id, command_id)
                                    if success:
                                        logger.info(f"[{request_id}] Command {command_id} acknowledged")
                                    else:
                                        logger.warning(f"[{request_id}] Failed to acknowledge command {command_id}")
                                
                                # Send the formatted binary command in response
                                logger.debug(f"[{request_id}] Command payload: {formatted_command}")

                                response = aiocoap.Message(mtype=aiocoap.ACK, code=aiocoap.Code.CREATED, token=request.token, payload=formatted_command)
                                logger.info(" response: " + str(response) + " payload: " + str(response.payload.hex()))
                                return response
                        except Exception as e:
                            logger.error(f"[{request_id}] Error handling pending commands: {e}")
                            # Continue with normal processing if command handling fails
                    logger.info(f"[{request_id}] Raw message published")
                    return aiocoap.Message(code=aiocoap.Code.CONTENT, payload=b"Raw message published")
                    
                except Exception as e:
                    logger.error(f"[{request_id}] Failed to create RawMessage: {e}")
            
        except Exception as e:
            logger.error(f"[{request_id}] Error processing request: {e}", exc_info=True)

        # For requests that don't match well-known or have device data, return appropriate response
        logger.warning(f"[{request_id}] Bad request - invalid endpoint or missing device data")
        return aiocoap.Message(code=aiocoap.Code.BAD_REQUEST, payload=b"Bad Request - Invalid endpoint or missing device data")

    # def _add_time_to_command(self, command_payload: bytes) -> bytes:
    #     """Add current time to command payload if it's a ProtoConfig."""
    #     try:
    #         from your_proto_module import ProtoConfig
            
    #         # Try to parse as ProtoConfig and add time
    #         config = ProtoConfig()
    #         config.ParseFromString(command_payload)
    #         config.current_time = int(time.time())
            
    #         return config.SerializeToString()
    #     except Exception:
    #         # If parsing fails, return original payload
    #         logger.warning("Could not add time to command payload")
    #         return command_payload
        
    def _extract_device_id_using_translation(self, request, request_id: str) -> TranslationResult:
        """Extract device ID using the translation layer."""
        try:
            # Create RawData object from the CoAP request
            uri_path = "/".join(list(request.opt.uri_path) if request.opt.uri_path else [])
            raw_data = RawData(
                protocol="coap",
                payload_bytes=request.payload,
                path=uri_path,
                metadata={
                    "method": request.code.name if hasattr(request.code, 'name') else str(request.code),
                    "source": request.remote.hostinfo if hasattr(request.remote, 'hostinfo') else str(request.remote),
                    "uri_path": uri_path,  # Keep for backward compatibility
                    "full_uri": request.get_request_uri() if hasattr(request, 'get_request_uri') else "unknown",
                    "request_id": request_id
                }
            )
            
            # Use translation manager to extract device ID
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
            return TranslationResult(
                success=False,
                error=error_msg
            )
