# coap_gateway/resources.py
import logging
from datetime import datetime, timezone

import aiocoap
import aiocoap.resource as resource

from shared.models.common import RawMessage # Import shared model
from shared.models.translation import RawData, TranslationResult
from shared.translation.manager import TranslationManager
from shared.translation.protobuf.translator import ProtobufTranslator
from shared.config_loader import get_translator_configs, validate_translator_config
from kafka_producer import KafkaMsgProducer # Import the Kafka producer wrapper
from command_consumer import CommandConsumer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
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
        
        logger.info("🚀 Initializing DataRootResource...")
        
        # Initialize translation manager
        self.translation_manager = self._initialize_translation_manager()
        
        logger.info(f"✅ DataRootResource initialized with translation manager.")

    def _initialize_translation_manager(self) -> TranslationManager:
        """Initialize the translation manager from YAML configuration."""
        try:
            # Get translator configurations for the CoAP connector
            translator_configs = get_translator_configs('coap_preservarium')
            
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
                config = translator_config.get('config', {})
                priority = translator_config.get('priority', 999)
                
                if translator_type == 'protobuf':
                    # Create Efento protobuf translator
                    translator = ProtobufTranslator(config=config)
                    translators.append(translator)
                    logger.info(f"Created {translator_type} translator with priority {priority}")
                else:
                    logger.warning(f"Unknown translator type: {translator_type}")
            
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
    


    async def render(self, request):
        """Monitor all incoming requests and log details."""
        self.request_count += 1
        request_id = f"REQ-{self.request_count:04d}"
        
        # Basic test log to confirm render method is called
        logger.info(f"🔄 Processing request {request_id}")
        
        try:
            # Extract request details
            method = request.code.name if hasattr(request.code, 'name') else str(request.code)
            source = request.remote.hostinfo if hasattr(request.remote, 'hostinfo') else str(request.remote)
            payload_size = len(request.payload) if request.payload else 0
            uri_path = list(request.opt.uri_path) if request.opt.uri_path else []
            full_uri = request.get_request_uri() if hasattr(request, 'get_request_uri') else "unknown"
            
            # Log comprehensive request details ALWAYS
            logger.info("=" * 80)
            logger.info(f"🔍 INCOMING CoAP REQUEST [{request_id}]")
            logger.info(f"  Method: {method}")
            logger.info(f"  Source: {source}")
            logger.info(f"  Full URI: {full_uri}")
            logger.info(f"  Path Components: {uri_path}")
            logger.info(f"  Payload Size: {payload_size} bytes")
            
            # Log and analyze payload if present
            translation_result = None
            if payload_size > 0:
                # Use translation manager to extract device ID
                translation_result = self._extract_device_id_using_translation(request, request_id)
                if translation_result and translation_result.success and translation_result.device_id:
                    logger.info(f"  🏷️  EXTRACTED DEVICE ID: '{translation_result.device_id}' (translator: {translation_result.translator_used})")
                elif translation_result and not translation_result.success:
                    logger.warning(f"  ⚠️  Translation failed: {translation_result.error}")
                
            else:
                logger.warning(f"  ⚠️  Empty payload - no device ID to extract")
                    
            logger.info("=" * 80)
            
            # Demo: Create RawMessage if device ID was extracted successfully
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
                            "uri_path": uri_path,
                            "translator_used": translation_result.translator_used,
                            "request_id": request_id
                        }
                    )
                    
                    logger.info(f"[{request_id}] ✅ Created RawMessage for device {translation_result.device_id}")
                    logger.debug(f"[{request_id}] RawMessage: {raw_message}")
                    
                    # In a real implementation, you would send this to Kafka:
                    await self.kafka_producer.publish_raw_message(raw_message)
                    
                except Exception as e:
                    logger.error(f"[{request_id}] Failed to create RawMessage: {e}")
            
            # For now, just return Method Not Allowed for all direct requests
            logger.warning(f"[{request_id}] Request received directly to root. Method Not Allowed.")
            return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED, payload=b"Direct root access not allowed")

        except Exception as e:
            logger.error(f"❌ Error processing request details for {request_id}: {e}", exc_info=True)

        # For now, just return Method Not Allowed for all direct requests
        logger.warning(f"[{request_id}] Request received directly to root. Method Not Allowed.")
        return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED, payload=b"Direct root access not allowed")

    def _extract_device_id_using_translation(self, request, request_id: str) -> TranslationResult:
        """Extract device ID using the translation layer."""
        try:
            # Create RawData object from the CoAP request
            raw_data = RawData(
                protocol="coap",
                payload_bytes=request.payload,
                metadata={
                    "method": request.code.name if hasattr(request.code, 'name') else str(request.code),
                    "source": request.remote.hostinfo if hasattr(request.remote, 'hostinfo') else str(request.remote),
                    "uri_path": list(request.opt.uri_path) if request.opt.uri_path else [],
                    "full_uri": request.get_request_uri() if hasattr(request, 'get_request_uri') else "unknown",
                    "request_id": request_id
                }
            )
            
            # Use translation manager to extract device ID
            result = self.translation_manager.extract_device_id(raw_data)
            
            if result.success:
                logger.info(f"[{request_id}] Translation successful using {result.translator_used}")
                if hasattr(result, 'parsed_data') and result.parsed_data:
                    logger.debug(f"[{request_id}] Parsed data: {result.parsed_data}")
            else:
                logger.warning(f"[{request_id}] Translation failed: {result.error}")

                    
            return result
            
        except Exception as e:
            error_msg = f"Translation extraction failed: {e}"
            logger.error(f"[{request_id}] {error_msg}")
            return TranslationResult(
                success=False,
                error=error_msg
            )
