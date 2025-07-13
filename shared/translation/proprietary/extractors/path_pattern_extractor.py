"""
Path pattern extractor for device ID extraction.

Supports extracting device IDs from path patterns in various protocols:
- MQTT topics (e.g., "broker_data/{device_id}/data")
- CoAP resource paths (e.g., "/sensors/{device_id}/readings")
- HTTP URLs (e.g., "/api/devices/{device_id}/status")
"""

import re
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class PathPatternExtractor:
    """
    Extracts device IDs from path patterns across different protocols.
    
    Supports patterns with placeholders like:
    - {device_id}
    - {building_id}
    - {sensor_id}
    - etc.
    
    Also supports protocol-specific wildcards:
    - MQTT: + (single level), # (multi-level)
    - Others: standard regex patterns
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the path pattern extractor.
        
        Args:
            config: Configuration containing pattern and other settings
        """
        self.config = config
        self.pattern = config.get('pattern', '')
        self.placeholder = config.get('placeholder', 'device_id')
        self.regex_flags = config.get('regex_flags', 0)
        
        # Convert pattern to regex
        self.regex_pattern = self._pattern_to_regex(self.pattern)
        logger.debug(f"Initialized PathPatternExtractor with pattern '{self.pattern}' -> regex '{self.regex_pattern}'")
    
    def extract_device_id(self, path: str) -> Optional[str]:
        """
        Extract device ID from the given path using the configured pattern.
        
        Args:
            path: The path to extract from (MQTT topic, CoAP path, etc.)
            
        Returns:
            Extracted device ID or None if not found
        """
        if not path or not self.regex_pattern:
            logger.debug(f"Empty path or pattern: path='{path}', pattern='{self.regex_pattern}'")
            return None
        
        logger.debug(f"Attempting to extract device ID from path: '{path}'")
        
        try:
            match = re.match(self.regex_pattern, path, self.regex_flags)
            if match:
                # Try to get the device_id group
                try:
                    device_id = match.group(self.placeholder)
                    logger.debug(f"Successfully extracted device ID '{device_id}' from path '{path}'")
                    return device_id
                except IndexError:
                    # If named group doesn't exist, try to get the first group
                    if match.groups():
                        device_id = match.group(1)
                        logger.debug(f"Extracted device ID '{device_id}' from first capture group")
                        return device_id
                    else:
                        logger.debug("No capture groups found in regex match")
                        return None
            else:
                logger.debug(f"Path '{path}' does not match pattern '{self.pattern}'")
                return None
                
        except re.error as e:
            logger.error(f"Regex error while matching path '{path}': {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting from path '{path}': {e}")
            return None
    
    def _pattern_to_regex(self, pattern: str) -> str:
        """
        Convert a pattern with placeholders to a regex pattern.
        
        Converts patterns like:
        - "broker_data/{device_id}/data" -> "broker_data/(?P<device_id>[^/]+)/data"
        - "/sensors/{device_id}/readings" -> "/sensors/(?P<device_id>[^/]+)/readings"
        - "topic/+/{device_id}/#" -> "topic/[^/]+/(?P<device_id>[^/]+)/.*"
        
        Args:
            pattern: Pattern string with placeholders
            
        Returns:
            Regex pattern string
        """
        if not pattern:
            return ''
        
        # Escape special regex characters except our placeholders and MQTT wildcards
        escaped = re.escape(pattern)
        
        # Convert MQTT wildcards first (before placeholder conversion)
        # + matches single level (no forward slashes)
        escaped = escaped.replace(r'\+', '[^/]+')
        # # matches multiple levels (including forward slashes)
        escaped = escaped.replace(r'\#', '.*')
        
        # Convert placeholders to named capture groups
        # Look for escaped braces around identifiers
        placeholder_pattern = r'\\{([a-zA-Z_][a-zA-Z0-9_]*)\\}'
        
        def replace_placeholder(match):
            placeholder_name = match.group(1)
            # Use [^/]+ to match any characters except path separators
            return f'(?P<{placeholder_name}>[^/]+)'
        
        regex_pattern = re.sub(placeholder_pattern, replace_placeholder, escaped)
        
        # Ensure the pattern matches the entire string
        if not regex_pattern.startswith('^'):
            regex_pattern = '^' + regex_pattern
        if not regex_pattern.endswith('$'):
            regex_pattern = regex_pattern + '$'
        
        logger.debug(f"Converted pattern '{pattern}' to regex '{regex_pattern}'")
        return regex_pattern 