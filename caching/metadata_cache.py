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
    Service for caching device metadata.
    Provides methods for storing and retrieving device readings.
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
                           ttl: Optional[int] = None) -> bool:
        """
        Cache a single reading for a device. If the reading has a new request_id,
        previous readings for this device will be cleared before adding this one.
        
        Args:
            device_id: The device ID
            reading_data: ValidatedOutput object containing reading data and metadata
            ttl: Time-to-live in seconds (optional)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract request_id from reading data
            request_id = reading_data.request_id
            
            # Store the reading data as JSON using the model's built-in serialization
            reading_metadata = reading_data.metadata
            logger.info(f"Caching reading for device {device_id}: {reading_metadata}")
            if not request_id:
                logger.warning(f"Reading data for device {device_id} has no request_id")
                # Still proceed with caching, but without request tracking
            else:
                # Check if this is a new request for this device
                current_request_key = f"device:{device_id}:current_request_id"
                current_request_id = await self.redis_repository.redis_client.get(current_request_key)
                logger.info(f"Current request_id for device {device_id}: {current_request_id}")
                # If request_id is different, clear previous readings
                if current_request_id is None or current_request_id != request_id:
                    logger.debug(f"New request_id {request_id} for device {device_id}, clearing previous readings")
                    
                    # Clear previous readings
                    readings_key = f"device:{device_id}:readings"
                    await self.redis_repository.redis_client.delete(readings_key)
                    
                    # Update current request ID
                    await self.redis_repository.redis_client.set(
                        current_request_key, 
                        request_id, 
                        ex=ttl or self.config.metadata_ttl
                    )
            
            # Add reading to the device's readings list
            readings_key = f"device:{device_id}:readings"
            await self.redis_repository.redis_client.rpush(readings_key, json.dumps(reading_metadata))
            
            # Set expiration on readings list
            await self.redis_repository.redis_client.expire(
                readings_key, 
                ttl or self.config.metadata_ttl
            )
            
            # Add device_id to the set of all devices
            await self.redis_repository.redis_client.sadd(self.config.devices_key, device_id)
            
            logger.debug(f"Cached reading for device {device_id}" + 
                        (f" with request_id {request_id}" if request_id else ""))
            return True
                
        except Exception as e:
            logger.error(f"Error caching reading for device {device_id}: {str(e)}")
            return False
    
    async def get_device_readings(self, device_id: str) -> List[Dict[str, Any]]:
        """
        Get all readings for a device from its most recent request.
        
        Args:
            device_id: The device ID
        
        Returns:
            List of reading data dictionaries
        """
        try:
            readings_key = f"device:{device_id}:readings"
            reading_jsons = await self.redis_repository.redis_client.lrange(readings_key, 0, -1)
            
            if not reading_jsons:
                logger.debug(f"No readings found for device {device_id}")
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
            
            return readings
            
        except Exception as e:
            logger.error(f"Error retrieving readings for device {device_id}: {str(e)}")
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
    
    async def close(self) -> None:
        """
        Close the cache connection.
        """
        try:
            await self.redis_repository.close()
        except Exception as e:
            logger.error(f"Error closing cache connection: {str(e)}") 