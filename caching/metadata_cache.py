import logging
import time
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from preservarium_sdk.infrastructure.redis_repository.redis_base_repository import RedisBaseRepository
from preservarium_sdk.core.config import RedisSettings
from shared.models.common import ValidatedOutput

logger = logging.getLogger("cache_service.metadata_cache")


class MetadataCache:
    """
    Service for caching complete device reading data (values, label, index, metadata).
    Provides methods for storing and retrieving device readings grouped by device_id and category.
    Utilizes RedisBaseRepository from SDK for basic Redis operations.
    """
    
    def __init__(self, redis_repository: RedisBaseRepository):
        """
        Initialize the device metadata cache service with a Redis repository.
        
        Args:
            redis_repository: A RedisBaseRepository implementation
        """
        self.redis_repository = redis_repository
        self.config = redis_repository.config
    
    async def cache_reading(self, device_id: str, reading_data: ValidatedOutput, 
                           category: str, ttl: Optional[int] = None) -> bool:
        """
        Cache a single reading for a device grouped by category. If the reading has a new request_id
        for this device+category combination, previous readings for this device+category will be cleared 
        before adding this one.
        
        Args:
            device_id: The device ID
            reading_data: ValidatedOutput object containing reading data and metadata
            category: The category from the datatype (e.g., "P", "M", "C", "S")
            ttl: Time-to-live in seconds (optional)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract request_id and index from reading data
            request_id = reading_data.request_id
            index = reading_data.index
            
            if not category:
                logger.warning(f"Reading data for device {device_id} has no category")
                return False
            
            # Create complete reading data dictionary including values, label, index, and metadata
            complete_reading_data = {
                "device_id": reading_data.device_id,
                "values": reading_data.values,
                "label": reading_data.label,
                "index": reading_data.index,
                "metadata": reading_data.metadata,
                "timestamp": reading_data.timestamp.isoformat() if reading_data.timestamp else None,
                "request_id": reading_data.request_id,
                "category": category,
                "display_name": self._parse_display_name(reading_data.metadata)
            }
            
            logger.info(f"Caching complete reading for device {device_id}, category {category}: values={reading_data.values}, label={reading_data.label}, metadata={reading_data.metadata}")
            
            if not request_id:
                logger.warning(f"Reading data for device {device_id}, category {category} has no request_id")
                # Still proceed with caching, but without request tracking
            else:
                # Check if this is a new request for this device+category combination
                current_request_key = f"device:{device_id}:category:{category}:current_request_id"
                current_request_id = await self.redis_repository.redis_client.get(current_request_key)
                logger.info(f"Current request_id for device {device_id}, category {category}: {current_request_id}")
                
                # If request_id is different, clear previous readings for this device+category
                if current_request_id is None or current_request_id != request_id:
                    logger.debug(f"New request_id {request_id} for device {device_id}, category {category}, clearing previous readings")
                    
                    # Clear previous readings for this device+category combination
                    readings_key = f"device:{device_id}:category:{category}:readings"
                    await self.redis_repository.redis_client.delete(readings_key)
                    
                    # Update current request ID for this device+category
                    await self.redis_repository.redis_client.set(
                        current_request_key, 
                        request_id, 
                        ex=ttl or self.config.metadata_ttl
                    )
            
            # Add complete reading to the device+category readings list
            readings_key = f"device:{device_id}:category:{category}:readings"
            await self.redis_repository.redis_client.rpush(readings_key, json.dumps(complete_reading_data))
            
            # Set expiration on readings list
            await self.redis_repository.redis_client.expire(
                readings_key, 
                ttl or self.config.metadata_ttl
            )
            
            # Add device_id to the set of all devices
            await self.redis_repository.redis_client.sadd(self.config.devices_key, device_id)
            
            # Add category to the set of categories for this device
            device_categories_key = f"device:{device_id}:categories"
            await self.redis_repository.redis_client.sadd(device_categories_key, category)
            await self.redis_repository.redis_client.expire(
                device_categories_key, 
                ttl or self.config.metadata_ttl
            )
            
            logger.debug(f"Cached complete reading for device {device_id}, category {category}" + 
                        (f" with request_id {request_id}" if request_id else ""))
            return True
                
        except Exception as e:
            logger.error(f"Error caching reading for device {device_id}, category {category}: {str(e)}")
            return False
    
    async def get_device_readings_by_category(self, device_id: str, category: str) -> List[Dict[str, Any]]:
        """
        Get all complete readings (values, label, index, metadata) for a specific device and category 
        from the most recent request for that device+category combination.
        
        Args:
            device_id: The device ID
            category: The category (e.g., "P", "M", "C", "S")
        
        Returns:
            List of complete reading data dictionaries containing values, label, index, metadata, etc.
        """
        try:
            readings_key = f"device:{device_id}:category:{category}:readings"
            reading_jsons = await self.redis_repository.redis_client.lrange(readings_key, 0, -1)
            
            if not reading_jsons:
                logger.debug(f"No readings found for device {device_id}, category {category}")
                return []
            
            # Parse JSON strings back to dictionaries
            readings = []
            for reading_json in reading_jsons:
                try:
                    if reading_json:
                        reading = json.loads(reading_json)
                        readings.append(reading)
                except json.JSONDecodeError:
                    logger.error(f"Error parsing reading JSON: {reading_json}")
            
            logger.debug(f"Retrieved {len(readings)} complete readings for device {device_id}, category {category}")
            return readings
            
        except Exception as e:
            logger.error(f"Error retrieving readings for device {device_id}, category {category}: {str(e)}")
            return []
    
    async def get_device_readings(self, device_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all complete readings for a device, grouped by category.
        
        Args:
            device_id: The device ID
        
        Returns:
            Dictionary where keys are categories and values are lists of reading data
        """
        try:
            # Get all categories for this device
            device_categories_key = f"device:{device_id}:categories"
            categories = await self.redis_repository.redis_client.smembers(device_categories_key)
            
            if not categories:
                logger.debug(f"No categories found for device {device_id}")
                return {}
            
            # Get readings for each category
            all_readings = {}
            for category in categories:
                readings = await self.get_device_readings_by_category(device_id, category)
                if readings:
                    all_readings[category] = readings
            
            logger.debug(f"Retrieved readings for device {device_id} with {len(all_readings)} categories")
            return all_readings
            
        except Exception as e:
            logger.error(f"Error retrieving all readings for device {device_id}: {str(e)}")
            return {}
    
    async def get_device_categories(self, device_id: str) -> List[str]:
        """
        Get all available categories for a device.
        
        Args:
            device_id: The device ID
        
        Returns:
            List of available categories for the device
        """
        try:
            device_categories_key = f"device:{device_id}:categories"
            categories = await self.redis_repository.redis_client.smembers(device_categories_key)
            return list(categories) if categories else []
        except Exception as e:
            logger.error(f"Error retrieving categories for device {device_id}: {str(e)}")
            return []
    
    async def get_all_device_ids(self) -> List[str]:
        """
        Get all device IDs stored in the cache.
        
        Returns:
            List of device IDs
        """
        try:
            return await self.redis_repository.redis_client.smembers(self.config.devices_key)
        except Exception as e:
            logger.error(f"Error retrieving all device IDs: {str(e)}")
            return []
    
    def _parse_display_name(self, metadata: Optional[Dict[str, Any]]) -> Optional[List[str]]:
        """
        Parse display_name from metadata JSON string to Python list.
        
        Args:
            metadata: The metadata dictionary containing datatype_display_name
            
        Returns:
            List of display names or None if not available/parseable
        """
        if not metadata or not metadata.get("datatype_display_name"):
            return None
            
        display_name_raw = metadata.get("datatype_display_name")
        
        # If it's already a list, return as is
        if isinstance(display_name_raw, list):
            return display_name_raw
            
        # If it's a string, try to parse as JSON
        if isinstance(display_name_raw, str):
            try:
                parsed = json.loads(display_name_raw)
                # Ensure it's a list
                if isinstance(parsed, list):
                    return parsed
                else:
                    logger.warning(f"Parsed display_name is not a list: {type(parsed)}")
                    return None
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse display_name JSON: {display_name_raw}, error: {e}")
                return None
                
        logger.warning(f"Unexpected display_name type: {type(display_name_raw)}")
        return None
    
    async def close(self) -> None:
        """
        Close the cache connection.
        """
        try:
            await self.redis_repository.close()
        except Exception as e:
            logger.error(f"Error closing cache connection: {str(e)}") 