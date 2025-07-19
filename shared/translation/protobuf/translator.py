from typing import Optional, Dict, Any
import logging
import os
from shared.translation.base import BaseTranslator
from shared.models.translation import RawData, TranslationResult
from .message_parser import ProtobufMessageParser
from .device_id_extractor import ProtobufDeviceIdExtractor
from shared.utils.script_client import ScriptClient, ScriptNotFoundError

logger = logging.getLogger(__name__)

class ProtobufTranslator(BaseTranslator):
    """Generic protobuf translator that works with any manufacturer's schemas."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.manufacturer = config.get('manufacturer', 'unknown')

        # Initialize components with manufacturer-specific config
        self.message_parser = ProtobufMessageParser(
            manufacturer=self.manufacturer,
            message_types=config.get('message_types', {})
        )

        self.device_id_extractor = ProtobufDeviceIdExtractor(
            config.get('device_id_extraction', {})
        )
        
        # Initialize script client for parser script loading
        self.script_client = ScriptClient(
            storage_type="local",
            local_dir="/app/storage/parser_scripts"  # Default parser scripts directory
        )
        
        # Cache for loaded script module
        self._script_module = None

    @property
    def parser_script_path(self) -> Optional[str]:
        """Get the parser script path from configuration."""
        return self.config.get('parser_script_path')

    @property
    def script_module(self):
        """Get the loaded parser script module (lazy loading)."""
        if self._script_module is None:
            self._script_module = self._load_script_module()
        return self._script_module

    def _load_script_module(self):
        """Load the parser script module from the configured path."""
        if not self.parser_script_path:
            logger.warning(f"No parser script path configured for {self.manufacturer} translator")
            return None
            
        try:
            # Build full script path
            script_path = os.path.join(self.script_client.local_dir, self.parser_script_path)
            
            # Use async wrapper for sync loading
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context, but get_module is async
                # For now, we'll handle this synchronously but this could be improved
                logger.warning("Loading script module synchronously from async context")
                return None
            else:
                # We're in a sync context, can use asyncio.run
                script_module = asyncio.run(self.script_client.get_module(script_path))
                return script_module
                
        except ScriptNotFoundError as e:
            logger.error(f"Parser script not found for {self.manufacturer} translator: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading parser script for {self.manufacturer} translator: {e}")
            return None

    async def load_script_module_async(self):
        """Async method to load the parser script module."""
        if not self.parser_script_path:
            logger.warning(f"No parser script path configured for {self.manufacturer} translator")
            return None
            
        try:
            # Build full script path
            script_path = os.path.join(self.script_client.local_dir, self.parser_script_path)
            
            # Load the script module
            self._script_module = await self.script_client.get_module(script_path)
            logger.info(f"Successfully loaded parser script module '{self.parser_script_path}' for {self.manufacturer} translator")
            return self._script_module
            
        except ScriptNotFoundError as e:
            logger.error(f"Parser script not found for {self.manufacturer} translator: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading parser script for {self.manufacturer} translator: {e}")
            return None

    def has_script_module(self) -> bool:
        """Check if translator has a valid script module."""
        return self.script_module is not None

    def can_handle(self, raw_data: RawData) -> bool:
        """Check if this translator can handle the protobuf data."""
        # Quick protobuf format detection
        if not self._is_likely_protobuf(raw_data.payload_bytes):
            return False

        # Try to parse with manufacturer's schemas
        try:
            message_type = self.message_parser.detect_message_type(raw_data.payload_bytes)
            return message_type is not None
        except Exception:
            return False

    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """
        Extract device ID from protobuf data using configured extraction sources.
        Uses CoAP path mapping if available, otherwise falls back to priority-based detection.
        
        Args:
            raw_data: Raw message data containing protobuf payload
            
        Returns:
            TranslationResult with extracted device_id or error details
        """
        logger.debug(f"Starting device ID extraction for {self.manufacturer} protobuf data")
        
        try:
            # Check for path mapping first
            path_mapping = self.config.get('path_mapping', {})
            mapped_message_type = None
            
            if path_mapping and raw_data.path:
                # For all protocols, path is a string
                # For CoAP: path is typically a single component like "i", "m", "c"
                # For MQTT: path is a topic string like "broker_data/building1/device123/data"
                # Try to map the whole path first, then first component if it contains '/'
                
                path_component = raw_data.path
                mapped_message_type = path_mapping.get(path_component)
                
                # If no direct match and path contains '/', try first component
                if not mapped_message_type and '/' in raw_data.path:
                    path_component = raw_data.path.split('/')[0]
                    mapped_message_type = path_mapping.get(path_component)
                
                if mapped_message_type:
                    logger.info(f"🗺️  Path '{path_component}' mapped to message type '{mapped_message_type}'")
            
            # Try mapped message type first if available
            if mapped_message_type:
                try:
                    logger.debug(f"Attempting to parse as mapped message type: {mapped_message_type}")
                    message_type, parsed_message = self.message_parser.parse_message_as_type(
                        raw_data.payload_bytes, mapped_message_type
                    )
                    
                    if message_type:
                        device_id = self.device_id_extractor.extract(message_type, parsed_message)
                        
                        if device_id:
                            logger.debug(f"✅ Device ID extracted using path mapping: {device_id}")
                            # Get device_type and action from the extraction source if available
                            device_type = self.device_id_extractor.get_device_type_for_source(self.device_id_extractor.last_source_used)
                            action = self.device_id_extractor.get_action_for_source(self.device_id_extractor.last_source_used)
                            return TranslationResult(
                                success=True,
                                device_id=device_id,
                                device_type=device_type,
                                action=action,
                                translator_used=f"protobuf_{self.manufacturer}",
                                translator_type="protobuf",
                                translator=self,
                                metadata={
                                    "manufacturer": self.manufacturer,
                                    "message_type": message_type,
                                    "extraction_source": self.device_id_extractor.last_source_used,
                                    "path_mapping_used": True,
                                    "path_component": path_component,
                                    "original_path": raw_data.path
                                }
                            )
                        else:
                            logger.warning(f"⚠️  Mapped message type '{mapped_message_type}' parsed but no device ID found")
                            
                except Exception as e:
                    logger.warning(f"⚠️  Path mapping failed for '{mapped_message_type}': {e}")
                    
            # Fallback to original priority-based detection
            logger.info(f"🔄 Falling back to priority-based message type detection")
            logger.debug(f"Parsing protobuf payload of {len(raw_data.payload_bytes)} bytes")
            message_type, parsed_message = self.message_parser.parse_message(
                raw_data.payload_bytes
            )
            logger.info(f"Successfully parsed {message_type} message using priority detection")
            # Extract device ID using configured sources
            logger.debug(f"Extracting device ID from {message_type} message")
            device_id = self.device_id_extractor.extract(message_type, parsed_message)

            if device_id:
                logger.info(f"Successfully extracted device ID: {device_id}")
                # Get device_type and action from the extraction source if available
                device_type = self.device_id_extractor.get_device_type_for_source(self.device_id_extractor.last_source_used)
                action = self.device_id_extractor.get_action_for_source(self.device_id_extractor.last_source_used)
                return TranslationResult(
                    success=True,
                    device_id=device_id,
                    device_type=device_type,
                    action=action,
                    translator_used=f"protobuf_{self.manufacturer}",
                    translator_type="protobuf",
                    translator=self,
                    metadata={
                        "manufacturer": self.manufacturer,
                        "message_type": message_type,
                        "extraction_source": self.device_id_extractor.last_source_used,
                        "path_mapping_used": False
                    }
                )
            else:
                error_msg = f"No device ID found in {self.manufacturer} {message_type} message"
                logger.warning(error_msg)
                
                # Log additional debugging info
                if hasattr(parsed_message, 'DESCRIPTOR'):
                    available_fields = [field.name for field in parsed_message.DESCRIPTOR.fields]
                    logger.debug(f"Available fields in {message_type}: {available_fields}")
                
                configured_sources = [
                    f"{s.get('message_type', 'unknown')}.{s.get('field_path', 'unknown')}" 
                    for s in self.device_id_extractor.sources
                ]
                logger.debug(f"Configured extraction sources: {configured_sources}")
                
                return TranslationResult(
                    success=False,
                    error=error_msg,
                    metadata={
                        "manufacturer": self.manufacturer,
                        "message_type": message_type
                    }
                )

        except Exception as e:
            error_msg = f"Protobuf parsing error: {str(e)}"
            logger.error(f"Protobuf parsing failed for {self.manufacturer}: {str(e)}", exc_info=True)
            return TranslationResult(
                success=False,
                error=error_msg
            )

    def _is_likely_protobuf(self, payload: bytes) -> bool:
        """Quick check if payload looks like protobuf."""
        if len(payload) < 2:
            return False
        # Protobuf typically starts with field tags
        return payload[0] in [0x08, 0x0a, 0x10, 0x12, 0x18, 0x1a, 0x20, 0x22] 