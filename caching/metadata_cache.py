import logging
from typing import Dict, Any, Optional
from datetime import datetime
from shared.models.common import ValidatedOutput
from redis_client import RedisClient

logger = logging.getLogger("cache_service.metadata_cache")

class MetadataCache:
    """
    Handles extraction of metadata from ValidatedOutput objects
    and caching in Redis.
    """
    
    def __init__(self, redis_client: RedisClient):
        """
        Initialize the metadata cache with a Redis client.
        
        Args:
            redis_client: An initialized RedisClient instance
        """
        self.redis_client = redis_client
        
    def cache_metadata(self, validated_data: ValidatedOutput) -> bool:
        """
        Extract metadata from ValidatedOutput and cache it in Redis.
        
        Args:
            validated_data: ValidatedOutput object from the validator
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            device_id = validated_data.device_id
            metadata = validated_data.metadata
            
            # Skip if no metadata or missing required fields
            if not metadata:
                logger.warning(f"No metadata in validated data for device {device_id}")
                return False
                
            # Create a copy of metadata to avoid modifying the original
            metadata_copy = dict(metadata)
            
            # Add timestamp if not present
            if isinstance(validated_data.timestamp, datetime):
                metadata_copy['timestamp'] = validated_data.timestamp.isoformat()
            else:
                metadata_copy['timestamp'] = str(validated_data.timestamp)
            
            # Store in Redis
            success = self.redis_client.store_device_metadata(device_id, metadata_copy)
            
            if success:
                logger.debug(f"Cached metadata for device {device_id}")
            else:
                logger.error(f"Failed to cache metadata for device {device_id}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error caching metadata: {str(e)}")
            return False
    
    def get_device_metadata(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a device from the cache.
        
        Args:
            device_id: The device ID
            
        Returns:
            Dictionary with metadata or None if not found/error
        """
        return self.redis_client.get_device_metadata(device_id)
    
    def get_all_device_ids(self) -> list:
        """
        Get list of all device IDs in the cache.
        
        Returns:
            List of device IDs
        """
        return self.redis_client.get_all_device_ids() 