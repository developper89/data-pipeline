import logging
import redis
import time
from typing import Dict, Any, List, Optional, Union
import config

logger = logging.getLogger("cache_service.redis_client")

class RedisClient:
    """
    Handles connection to Redis and provides methods for
    storing and retrieving device metadata.
    """
    
    def __init__(self):
        self.host = config.REDIS_HOST
        self.port = config.REDIS_PORT
        self.db = config.REDIS_DB
        self.password = config.REDIS_PASSWORD
        self.metadata_ttl = config.REDIS_METADATA_TTL
        self.client = None
        self._connect()
        
    def _connect(self) -> None:
        """
        Establishes connection to Redis with retry mechanism.
        """
        retry_count = 0
        max_retries = 5
        retry_delay = 1  # seconds
        
        while retry_count < max_retries:
            try:
                logger.info(f"Connecting to Redis at {self.host}:{self.port} (DB: {self.db})")
                
                connection_kwargs = {
                    'host': self.host,
                    'port': self.port,
                    'db': self.db,
                    'decode_responses': True,  # Automatically decode responses to Python strings
                }
                
                # Add password if provided
                if self.password:
                    connection_kwargs['password'] = self.password
                
                self.client = redis.Redis(**connection_kwargs)
                
                # Test connection
                self.client.ping()
                logger.info("Successfully connected to Redis")
                return
                
            except redis.RedisError as e:
                retry_count += 1
                logger.error(f"Failed to connect to Redis (attempt {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    # Exponential backoff
                    retry_delay *= 2
                else:
                    logger.error("Max retries reached. Could not connect to Redis.")
                    raise
    
    def store_device_metadata(self, device_id: str, metadata: Dict[str, Any]) -> bool:
        """
        Stores device metadata in Redis.
        
        Args:
            device_id: The device ID
            metadata: Dictionary containing metadata
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create key for device metadata
            key = f"device:{device_id}:metadata"
            
            # Add current timestamp if not present
            if 'last_updated' not in metadata:
                metadata['last_updated'] = time.time()
            
            # Store metadata as a hash
            self.client.hset(key, mapping=metadata)
            
            # Set expiration time
            self.client.expire(key, self.metadata_ttl)
            
            # Add device_id to the set of all devices
            self.client.sadd("devices", device_id)
            
            logger.debug(f"Stored metadata for device {device_id}")
            return True
            
        except redis.RedisError as e:
            logger.error(f"Redis error storing metadata for device {device_id}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error storing metadata for device {device_id}: {str(e)}")
            return False
    
    def get_device_metadata(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves device metadata from Redis.
        
        Args:
            device_id: The device ID
        
        Returns:
            Dictionary with metadata or None if not found
        """
        try:
            key = f"device:{device_id}:metadata"
            metadata = self.client.hgetall(key)
            
            if not metadata:
                logger.debug(f"No metadata found for device {device_id}")
                return None
            
            return metadata
            
        except redis.RedisError as e:
            logger.error(f"Redis error retrieving metadata for device {device_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving metadata for device {device_id}: {str(e)}")
            return None
    
    def get_all_device_ids(self) -> List[str]:
        """
        Returns a list of all device IDs in the cache.
        
        Returns:
            List of device IDs
        """
        try:
            device_ids = self.client.smembers("devices")
            return list(device_ids)
            
        except redis.RedisError as e:
            logger.error(f"Redis error retrieving device list: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error retrieving device list: {str(e)}")
            return []
    
    def close(self) -> None:
        """
        Closes the Redis connection.
        """
        if self.client:
            try:
                self.client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {str(e)}")
                
    def __del__(self):
        """
        Ensures connection is closed when the object is garbage collected.
        """
        self.close() 