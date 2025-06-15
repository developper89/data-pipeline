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
        self.sources = config.get('device_id_extraction', {}).get('sources', [])
        
        # Sort sources by priority (lower number = higher priority)
        self.sources.sort(key=lambda x: x.get('priority', 999))
        
        logger.info(f"Initialized PatternTranslator with {len(self.sources)} extraction sources")
        for i, source in enumerate(self.sources):
            logger.debug(f"Source {i+1}: type={source.get('type')}, priority={source.get('priority', 999)}")
    
    def can_handle(self, raw_data: RawData) -> bool:
        """
        Check if this translator can handle the raw data.
        
        Args:
            raw_data: Raw data to check
            
        Returns:
            True if this translator can process the data
        """
        # Check if we have any path-based sources and a path to work with
        has_path_sources = any(source.get('type') == 'path_pattern' for source in self.sources)
        has_path_data = bool(getattr(raw_data, 'path', ''))
        
        # Check if we have any JSON payload sources and payload to work with
        has_json_sources = any(source.get('type') == 'json_payload' for source in self.sources)
        has_payload_data = bool(getattr(raw_data, 'payload_bytes', b''))
        
        # Can handle if we have matching sources and data
        can_handle_path = has_path_sources and has_path_data
        can_handle_json = has_json_sources and has_payload_data
        
        result = can_handle_path or can_handle_json
        logger.debug(f"PatternTranslator can_handle: {result} (path: {can_handle_path}, json: {can_handle_json})")
        
        return result
    
    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """
        Extract device ID from raw data using configured pattern sources.
        
        Args:
            raw_data: Raw data containing path and payload information
            
        Returns:
            TranslationResult with device ID or error information
        """
        logger.debug(f"Attempting to extract device ID from {len(self.sources)} sources")
        
        # Get path and payload from raw_data
        path = getattr(raw_data, 'path', '') or ''
        payload_bytes = getattr(raw_data, 'payload_bytes', b'')
        
        if isinstance(payload_bytes, str):
            payload_bytes = payload_bytes.encode('utf-8')
        
        logger.debug(f"Path: '{path}', Payload size: {len(payload_bytes)} bytes")
        
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
                    device_id = extractor.extract_device_id(payload_bytes)
                    
                else:
                    logger.warning(f"Unknown source type: {source_type}")
                    continue
                
                if device_id:
                    # Apply validation if configured
                    if self._validate_device_id(device_id, source):
                        logger.info(f"Successfully extracted device ID '{device_id}' using source {i+1} ({source_type})")
                        return TranslationResult(
                            device_id=device_id,
                            success=True,
                            raw_data=raw_data,
                            translator_type="pattern",
                            metadata={
                                'sources_count': len(self.sources),
                                'extraction_successful': device_id is not None,
                                'source_used': f"{source_type}_{i+1}"
                            }
                        ) 
                    else:
                        logger.debug(f"Device ID '{device_id}' failed validation for source {i+1}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Error in source {i+1} ({source_type}): {e}")
                continue
        
        logger.warning("No device ID could be extracted from any configured source")
        return TranslationResult(
            success=False,
            error="No device ID could be extracted from any configured source",
            raw_data=raw_data,
            translator_type="pattern",
            metadata={
                'sources_count': len(self.sources),
                'extraction_successful': False
            }
        )
    
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