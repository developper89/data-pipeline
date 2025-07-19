# shared/translation/protobuf/device_id_extractor.py
import re
import logging
from typing import Optional, Any, Dict, List

logger = logging.getLogger(__name__)

class ProtobufDeviceIdExtractor:
    """Generic device ID extractor that works with configured field mappings."""

    def __init__(self, extraction_config: Dict[str, Any]):
        self.sources = extraction_config.get('sources', [])
        self.validation = extraction_config.get('validation', {})
        self.last_source_used = None

        # Sort sources by priority
        self.sources = sorted(self.sources, key=lambda x: x.get('priority', 999))
        logger.debug(f"Initialized device ID extractor with {len(self.sources)} sources")

    def extract(self, message_type: str, parsed_message: Any) -> Optional[str]:
        """Extract device ID from parsed message using configured sources."""
        logger.debug(f"Extracting device ID from message type: {message_type}")

        # Log available fields in the message for debugging - make this INFO so it's always visible
        if hasattr(parsed_message, 'DESCRIPTOR'):
            available_fields = [field.name for field in parsed_message.DESCRIPTOR.fields]
            logger.debug(f"Available fields in {message_type}: {available_fields}")
        else:
            logger.info(f"Message type {message_type} has no DESCRIPTOR - available attributes: {dir(parsed_message)}")

        # Try configured sources in priority order
        for i, source in enumerate(self.sources):
            if source.get('message_type') != message_type:
                logger.debug(f"Skipping source {i+1}: message type '{source.get('message_type')}' != '{message_type}'")
                continue

            field_path = source.get('field_path')
            logger.debug(f"Trying extraction source {i+1}: {message_type}.{field_path}")
            
            device_id = self._extract_from_field_path(parsed_message, field_path, message_type)

            if device_id:
                if self._validate_device_id(device_id, field_path):
                    normalized_id = self._normalize_device_id(device_id)
                    self.last_source_used = f"{message_type}.{field_path}"
                    logger.debug(f"Successfully extracted device ID '{normalized_id}' from {self.last_source_used}")
                    return normalized_id
                else:
                    logger.warning(f"Device ID '{device_id}' from {message_type}.{field_path} failed validation")
            else:
                logger.info(f"No device ID found in {message_type}.{field_path}")

        # Make this INFO level so configuration issues are always visible
        configured_sources = [f"{s.get('message_type')}.{s.get('field_path')}" for s in self.sources]
        logger.info(f"No valid device ID found in {message_type} message. Tried sources: {configured_sources}")
        return None

    def _extract_from_field_path(self, message: Any, field_path: str, message_type: str = "") -> Optional[str]:
        """Extract value from nested field path (e.g., 'device.info.id')."""
        try:
            current = message
            path_parts = field_path.split('.')
            
            for i, field in enumerate(path_parts):
                if hasattr(current, field):
                    current = getattr(current, field)
                    logger.debug(f"Found field '{field}' at path level {i+1}")
                else:
                    # Log available fields when field is not found - make this INFO so it's always visible
                    if hasattr(current, 'DESCRIPTOR'):
                        available_fields = [f.name for f in current.DESCRIPTOR.fields]
                        logger.info(f"Field '{field}' not found in {message_type} message at path '{'.'.join(path_parts[:i+1])}'")
                        logger.info(f"Available fields at this level: {available_fields}")
                    else:
                        logger.info(f"Field '{field}' not found in {message_type} message at path '{'.'.join(path_parts[:i+1])}'")
                        logger.info(f"Object type: {type(current)}, available attributes: {dir(current)}")
                    return None

            # Convert to string and log the result
            if isinstance(current, (str, bytes)):
                if isinstance(current, bytes):
                    # Handle binary data - convert to hex string for device IDs
                    if len(current) > 0:
                        result = current.hex()
                        logger.debug(f"Extracted bytes from {field_path}: {len(current)} bytes -> hex: '{result}'")
                        return result
                    else:
                        logger.debug(f"Extracted empty bytes from {field_path}")
                        return None
                else:
                    result = current
                    logger.debug(f"Extracted string from {field_path}: '{result}'")
                    return result
            else:
                result = str(current)
                logger.debug(f"Extracted value from {field_path}: '{result}' (converted from {type(current).__name__})")
                return result

        except Exception as e:
            logger.error(f"Exception while extracting from field path '{field_path}' in {message_type}: {e}")
            return None

    def _validate_device_id(self, device_id: str, field_path: str = "") -> bool:
        """Validate device ID using configured regex."""
        if not device_id or len(device_id) < 3:
            logger.warning(f"Device ID validation failed: too short ('{device_id}', length: {len(device_id) if device_id else 0})")
            return False

        regex_pattern = self.validation.get('regex')
        if regex_pattern:
            if re.match(regex_pattern, device_id):
                logger.debug(f"Device ID '{device_id}' passed custom regex validation: {regex_pattern}")
                return True
            else:
                logger.warning(f"Device ID '{device_id}' failed custom regex validation: {regex_pattern}")
                return False

        # Default validation for common formats
        default_pattern = r'^[a-zA-Z0-9:_-]{3,32}$'
        if re.match(default_pattern, device_id):
            logger.debug(f"Device ID '{device_id}' passed default validation")
            return True
        else:
            logger.warning(f"Device ID '{device_id}' failed default validation: {default_pattern}")
            return False

    def _normalize_device_id(self, device_id: str) -> str:
        """Normalize device ID using configured rules."""
        original_id = device_id
        
        # Remove configured prefix
        prefix_to_remove = self.validation.get('remove_prefix')
        if prefix_to_remove and device_id.startswith(prefix_to_remove):
            device_id = device_id[len(prefix_to_remove):]
            logger.debug(f"Removed prefix '{prefix_to_remove}': '{original_id}' -> '{device_id}'")

        # Apply normalization
        if self.validation.get('normalize', True):
            device_id = device_id.lower()
            if device_id != original_id:
                logger.debug(f"Normalized device ID: '{original_id}' -> '{device_id}'")

        return device_id
    
    def get_device_type_for_source(self, source_used: str) -> Optional[str]:
        """
        Get device_type from the source configuration that was used for extraction.
        
        Args:
            source_used: Source identifier like "measurements.serial_num"
            
        Returns:
            Device type or None if not found
        """
        if not source_used:
            return None
            
        # Parse the source identifier (e.g., "measurements.serial_num")
        parts = source_used.split('.')
        if len(parts) < 2:
            return None
            
        message_type = parts[0]
        field_path = '.'.join(parts[1:])
        
        # Find the matching source configuration
        for source in self.sources:
            if (source.get('message_type') == message_type and 
                source.get('field_path') == field_path):
                device_type = source.get('device_type')
                if device_type:
                    logger.debug(f"Found device_type '{device_type}' for source '{source_used}'")
                    return device_type
                    
        logger.debug(f"No device_type found for source '{source_used}'")
        return None

    def get_action_for_source(self, source_used: str) -> Optional[str]:
        """
        Get action from the source configuration that was used for extraction.
        
        Args:
            source_used: Source identifier like "measurements.serial_num"
            
        Returns:
            Action/method name or None if not found
        """
        if not source_used:
            return None
            
        # Parse the source identifier (e.g., "measurements.serial_num")
        parts = source_used.split('.')
        if len(parts) < 2:
            return None
            
        message_type = parts[0]
        field_path = '.'.join(parts[1:])
        
        # Find the matching source configuration
        for source in self.sources:
            if (source.get('message_type') == message_type and 
                source.get('field_path') == field_path):
                action = source.get('action')
                if action:
                    logger.debug(f"Found action '{action}' for source '{source_used}'")
                    return action
                    
        logger.debug(f"No action found for source '{source_used}'")
        return None 