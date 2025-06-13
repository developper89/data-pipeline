"""
Protocol-agnostic pattern-based device ID translator.

This translator extracts device IDs from patterns in paths (MQTT topics, CoAP resource paths, 
HTTP URLs, etc.) and JSON payloads. It supports multiple extraction sources with priority-based
selection and validation.
"""

import logging
from typing import Dict, List, Optional

from ..base import BaseTranslator
from ...models.translation import RawData, TranslationResult
from .extractors.path_pattern_extractor import PathPatternExtractor
from .extractors.json_payload_extractor import JsonPayloadExtractor

logger = logging.getLogger(__name__)


class PatternTranslator(BaseTranslator):
    """
    Protocol-agnostic pattern-based device ID translator.
    
    Supports extraction from:
    - Path patterns (MQTT topics, CoAP resource paths, HTTP URLs, etc.)
    - JSON payloads using JSONPath expressions
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the pattern translator.
        
        Args:
            config: Configuration dictionary containing device_id_extraction sources
        """
        super().__init__(config)
        self.sources = config.get('device_id_extraction', {}).get('sources', [])
        
        # Sort sources by priority (lower number = higher priority)
        self.sources.sort(key=lambda x: x.get('priority', 999))
        
        logger.info(f"Initialized PatternTranslator with {len(self.sources)} extraction sources")
        for i, source in enumerate(self.sources):
            logger.debug(f"Source {i+1}: type={source.get('type')}, priority={source.get('priority', 999)}")
    
    def extract_device_id(self, raw_data: RawData) -> Optional[str]:
        """
        Extract device ID from raw data using configured pattern sources.
        
        Args:
            raw_data: Raw data containing path and payload information
            
        Returns:
            Extracted device ID or None if not found
        """
        logger.debug(f"Attempting to extract device ID from {len(self.sources)} sources")
        
        # Get path and payload from raw_data
        path = getattr(raw_data, 'path', '') or getattr(raw_data, 'topic', '') or ''
        payload = getattr(raw_data, 'payload', b'')
        
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        
        logger.debug(f"Path: '{path}', Payload size: {len(payload)} bytes")
        
        for i, source in enumerate(self.sources):
            source_type = source.get('type')
            logger.debug(f"Trying source {i+1}: {source_type}")
            
            try:
                device_id = None
                
                if source_type == 'path_pattern':
                    # Extract from path pattern (MQTT topic, CoAP resource path, etc.)
                    extractor = PathPatternExtractor(source)
                    device_id = extractor.extract_device_id(path)
                    
                elif source_type == 'json_payload':
                    # Extract from JSON payload
                    extractor = JsonPayloadExtractor(source)
                    device_id = extractor.extract_device_id(payload)
                    
                else:
                    logger.warning(f"Unknown source type: {source_type}")
                    continue
                
                if device_id:
                    # Apply validation if configured
                    if self._validate_device_id(device_id, source):
                        logger.info(f"Successfully extracted device ID '{device_id}' using source {i+1} ({source_type})")
                        return device_id
                    else:
                        logger.debug(f"Device ID '{device_id}' failed validation for source {i+1}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Error in source {i+1} ({source_type}): {e}")
                continue
        
        logger.warning("No device ID could be extracted from any configured source")
        return None
    
    def _validate_device_id(self, device_id: str, source: Dict) -> bool:
        """
        Validate extracted device ID against source-specific rules.
        
        Args:
            device_id: The extracted device ID
            source: Source configuration
            
        Returns:
            True if validation passes
        """
        validation = source.get('validation', {})
        
        # Check minimum length
        min_length = validation.get('min_length')
        if min_length and len(device_id) < min_length:
            logger.debug(f"Device ID '{device_id}' too short (min: {min_length})")
            return False
            
        # Check maximum length  
        max_length = validation.get('max_length')
        if max_length and len(device_id) > max_length:
            logger.debug(f"Device ID '{device_id}' too long (max: {max_length})")
            return False
            
        # Check pattern matching
        pattern = validation.get('pattern')
        if pattern:
            import re
            if not re.match(pattern, device_id):
                logger.debug(f"Device ID '{device_id}' doesn't match pattern '{pattern}'")
                return False
        
        return True
    
    def translate(self, raw_data: RawData) -> TranslationResult:
        """
        Translate raw data by extracting device ID.
        
        Args:
            raw_data: Raw data to translate
            
        Returns:
            Translation result with extracted device ID
        """
        device_id = self.extract_device_id(raw_data)
        
        return TranslationResult(
            device_id=device_id,
            success=device_id is not None,
            raw_data=raw_data,
            translator_type="pattern",
            metadata={
                'sources_count': len(self.sources),
                'extraction_successful': device_id is not None
            }
        ) 