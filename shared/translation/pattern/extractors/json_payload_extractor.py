# shared/translation/pattern/extractors/json_payload_extractor.py
import json
import logging
from typing import Optional, Dict, Any, List, Union
from ....models.translation import RawData

logger = logging.getLogger(__name__)

class JsonPayloadExtractor:
    """
    Extractor that extracts device IDs from JSON payloads using JSONPath expressions.
    
    Supports JSONPath expressions like:
    - "$.device.id"
    - "$.deviceId" 
    - "$.sensor.identifier"
    - Multiple fallback paths
    """
    
    def extract(self, raw_data: RawData, source_config: Dict[str, Any]) -> Optional[str]:
        """
        Extract device ID from JSON payload using JSONPath expressions.
        
        Args:
            raw_data: RawData containing MQTT payload
            source_config: Configuration containing:
                - json_path: Primary JSONPath expression
                - fallback_paths: List of fallback JSONPath expressions
                
        Returns:
            Extracted device ID or None if not found
        """
        json_path = source_config.get('json_path', '')
        fallback_paths = source_config.get('fallback_paths', [])
        
        if not json_path:
            logger.error("No json_path specified in json_payload source")
            return None
        
        # Parse payload as JSON
        try:
            if raw_data.payload_bytes:
                payload_str = raw_data.payload_bytes.decode('utf-8', errors='replace')
                payload_json = json.loads(payload_str)
                logger.debug(f"Successfully parsed JSON payload: {len(payload_str)} chars")
            else:
                logger.debug("No payload to parse")
                return None
                
        except json.JSONDecodeError as e:
            logger.debug(f"Failed to parse payload as JSON: {e}")
            return None
        except UnicodeDecodeError as e:
            logger.debug(f"Failed to decode payload as UTF-8: {e}")
            return None
        
        # Try primary JSONPath
        all_paths = [json_path] + fallback_paths
        
        for i, path in enumerate(all_paths):
            path_type = "primary" if i == 0 else f"fallback_{i}"
            logger.debug(f"Trying {path_type} JSONPath: {path}")
            
            device_id = self._extract_from_path(payload_json, path)
            if device_id:
                logger.debug(f"Extracted device_id '{device_id}' using {path_type} path: {path}")
                return device_id
        
        logger.debug(f"No device ID found using any of {len(all_paths)} JSONPath expressions")
        return None
    
    def _extract_from_path(self, data: Dict[str, Any], path: str) -> Optional[str]:
        """
        Extract value from JSON data using a simplified JSONPath expression.
        
        Args:
            data: Parsed JSON data
            path: JSONPath expression (simplified version)
            
        Returns:
            Extracted value as string or None if not found
        """
        try:
            # Handle simple JSONPath expressions
            if path.startswith('$.'):
                # Remove $. prefix and split by dots
                path_parts = path[2:].split('.')
            elif path.startswith('$'):
                # Remove $ prefix and split by dots  
                path_parts = path[1:].split('.')
            else:
                # Treat as direct path
                path_parts = path.split('.')
            
            current = data
            for part in path_parts:
                if part == '':
                    continue
                    
                # Handle array indexing like [0]
                if '[' in part and ']' in part:
                    field_name = part.split('[')[0]
                    index_str = part.split('[')[1].split(']')[0]
                    
                    if field_name and field_name in current:
                        current = current[field_name]
                    
                    try:
                        index = int(index_str)
                        if isinstance(current, list) and 0 <= index < len(current):
                            current = current[index]
                        else:
                            return None
                    except ValueError:
                        return None
                        
                elif isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    # Path not found
                    return None
            
            # Convert result to string
            if current is not None:
                if isinstance(current, (str, int, float)):
                    return str(current)
                else:
                    # For complex objects, try to find a reasonable string representation
                    logger.debug(f"JSONPath resulted in complex object: {type(current)}")
                    return str(current)
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting from JSONPath '{path}': {e}")
            return None 