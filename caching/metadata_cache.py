import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

from preservarium_sdk.infrastructure.redis_repository.redis_base_repository import RedisBaseRepository
from preservarium_sdk.core.config import RedisSettings

logger = logging.getLogger("cache_service.metadata_cache")


class MetadataCache:
    """
    Service for caching device metadata.
    Provides methods for storing and retrieving device metadata.
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
    
    async def cache_device_metadata(self, device_id: str, metadata: Dict[str, Any], 
                                   ttl: Optional[int] = None) -> bool:
        """
        Cache device metadata.
        
        Args:
            device_id: The device ID
            metadata: Dictionary containing metadata
            ttl: Time-to-live in seconds (optional)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create a copy of metadata to avoid modifying the original
            metadata_copy = dict(metadata)
            
            # Add timestamp if not present
            if 'last_updated' not in metadata_copy:
                if isinstance(metadata.get('timestamp'), datetime):
                    metadata_copy['last_updated'] = metadata['timestamp'].isoformat()
                else:
                    metadata_copy['last_updated'] = str(time.time())
            
            # Create key for device metadata using the configured prefix
            key = f"{self.config.device_metadata_key_prefix}{device_id}:metadata"
            
            # Store metadata as a hash
            success = await self.redis_repository.store(key, metadata_copy, ttl or self.config.metadata_ttl)
            
            if success:
                # Add device_id to the set of all devices
                await self.redis_repository.redis_client.sadd(self.config.devices_key, device_id)
                logger.debug(f"Stored metadata for device {device_id}")
                return True
            else:
                logger.error(f"Failed to store metadata for device {device_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error caching metadata for device {device_id}: {str(e)}")
            return False
    
    async def get_device_metadata(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve device metadata from the cache.
        
        Args:
            device_id: The device ID
        
        Returns:
            Dictionary with metadata or None if not found
        """
        try:
            key = f"{self.config.device_metadata_key_prefix}{device_id}:metadata"
            return await self.redis_repository.get(key)
        except Exception as e:
            logger.error(f"Error retrieving metadata for device {device_id}: {str(e)}")
            return None
    
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
    
    async def cache_validated_output(self, validated_output: Any) -> bool:
        """
        Cache metadata from a ValidatedOutput object.
        This method can be used to directly cache data from the validation pipeline.
        
        Args:
            validated_output: ValidatedOutput object with device_id, metadata, and timestamp
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract data from ValidatedOutput
            device_id = getattr(validated_output, 'device_id', None)
            metadata = getattr(validated_output, 'metadata', None)
            
            # Skip if no metadata or missing required fields
            if not device_id or not metadata:
                logger.warning(f"No device_id or metadata in validated data")
                return False
            
            # Create a copy of metadata
            metadata_copy = dict(metadata)
            
            # Add timestamp if available
            timestamp = getattr(validated_output, 'timestamp', None)
            if timestamp:
                if isinstance(timestamp, datetime):
                    metadata_copy['timestamp'] = timestamp.isoformat()
                else:
                    metadata_copy['timestamp'] = str(timestamp)
            
            # Cache the metadata
            return await self.cache_device_metadata(device_id, metadata_copy)
            
        except Exception as e:
            if hasattr(validated_output, 'device_id'):
                device_id = validated_output.device_id
                logger.error(f"Error caching validated output for device {device_id}: {str(e)}")
            else:
                logger.error(f"Error caching validated output: {str(e)}")
            return False
    
    async def close(self) -> None:
        """
        Close the cache connection.
        """
        try:
            await self.redis_repository.close()
        except Exception as e:
            logger.error(f"Error closing cache connection: {str(e)}") 