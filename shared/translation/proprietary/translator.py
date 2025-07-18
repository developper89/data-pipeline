"""
Protocol-agnostic pattern-based device ID translator.

This translator extracts device IDs from patterns in paths (MQTT topics, CoAP resource paths, 
HTTP URLs, etc.) and JSON payloads. It supports multiple extraction sources with priority-based
selection and validation.
"""

import logging
import os
from typing import Dict, List, Optional

from ..base import BaseTranslator
from ...models.translation import RawData, TranslationResult
from .extractors.path_pattern_extractor import PathPatternExtractor
from .extractors.json_payload_extractor import JsonPayloadExtractor
from shared.utils.script_client import ScriptClient, ScriptNotFoundError

logger = logging.getLogger(__name__)


class ProprietaryTranslator(BaseTranslator):
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
        self.config = config
        self.sources = config.get('device_id_extraction', {}).get('sources', [])
        self.manufacturer = config.get('manufacturer', 'unknown')
        
        # Sort sources by priority (lower number = higher priority)
        self.sources.sort(key=lambda x: x.get('priority', 999))
        
        logger.info(f"Initialized ProprietaryTranslator with {len(self.sources)} extraction sources")
        for i, source in enumerate(self.sources):
            logger.debug(f"Source {i+1}: type={source.get('type')}, priority={source.get('priority', 999)}")
            
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
            logger.warning(f"No parser script path configured for proprietary translator")
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
                logger.info(f"Successfully loaded parser script module '{self.parser_script_path}' for proprietary translator")
                return script_module
                
        except ScriptNotFoundError as e:
            logger.error(f"Parser script not found for proprietary translator: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading parser script for proprietary translator: {e}")
            return None

    async def load_script_module_async(self):
        """Async method to load the parser script module."""
        if not self.parser_script_path:
            logger.warning(f"No parser script path configured for proprietary translator")
            return None
            
        try:
            # Build full script path
            script_path = os.path.join(self.script_client.local_dir, self.parser_script_path)
            
            # Load the script module
            self._script_module = await self.script_client.get_module(script_path)
            logger.info(f"Successfully loaded parser script module '{self.parser_script_path}' for proprietary translator")
            return self._script_module
            
        except ScriptNotFoundError as e:
            logger.error(f"Parser script not found for proprietary translator: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading parser script for proprietary translator: {e}")
            return None

    def has_script_module(self) -> bool:
        """Check if translator has a valid script module."""
        return self.script_module is not None
    
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
        logger.debug(f"ProprietaryTranslator can_handle: {result} (path: {can_handle_path}, json: {can_handle_json})")
        
        return result
    
    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """
        Extract device ID from raw data using configured proprietary sources.
        
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
                        logger.debug(f"Successfully extracted device ID '{device_id}' using source {i+1} ({source_type})")
                        return TranslationResult(
                            success=True,
                            device_id=device_id,
                            device_type=source.get('device_type'),  # Read device_type from source config
                            translator_used=f"proprietary_{self.manufacturer}",
                            translator_type="proprietary",
                            translator=self,  # Pass self reference for manufacturer access
                            raw_data=raw_data,
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
            translator_type="proprietary",
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