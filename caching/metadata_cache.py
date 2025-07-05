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
        Cache a single reading for a device grouped by category and index. Only the latest record 
        for each category+index combination is stored, replacing any previous record with the same 
        category+index.
        
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
                
            if not index:
                logger.warning(f"Reading data for device {device_id}, category {category} has no index")
                return False
            
            # Create complete reading data dictionary including values, label, index, and metadata
            complete_reading_data = {
                "device_id": reading_data.device_id,
                "values": reading_data.values,
                "labels": reading_data.labels,
                "display_names": reading_data.display_names,
                "index": reading_data.index,
                "metadata": reading_data.metadata,
                "timestamp": reading_data.timestamp.isoformat() if reading_data.timestamp else None,
                "request_id": reading_data.request_id,
                "category": category,
            }
            
            logger.info(f"Caching latest reading for device {device_id}, category {category}, index {index}: values={reading_data.values}")
            
            # Use Redis hash to store latest reading for each category+index combination
            # Key structure: device:{device_id}:category:{category}:readings
            # Hash field: {index} -> JSON object containing the reading data directly
            readings_key = f"device:{device_id}:category:{category}:readings"
            
            # Store the reading data directly under the index key
            await self.redis_repository.redis_client.hset(
                readings_key, 
                index, 
                json.dumps(complete_reading_data)
            )
            
            # Set expiration on readings hash
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
            
            # Track the latest request_id for this device+category (for logging/debugging)
            if request_id:
                current_request_key = f"device:{device_id}:category:{category}:current_request_id"
                await self.redis_repository.redis_client.set(
                    current_request_key, 
                    request_id, 
                    ex=ttl or self.config.metadata_ttl
                )
            
            logger.debug(f"Cached latest reading for device {device_id}, category {category}, index {index}" + 
                        (f" with request_id {request_id}" if request_id else ""))
            return True
                
        except Exception as e:
            logger.error(f"Error caching reading for device {device_id}, category {category}, index {index}: {str(e)}")
            return False
    
    async def get_device_reading_by_category_and_index(self, device_id: str, category: str, index: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest reading for a specific device, category, and index combination.
        
        Args:
            device_id: The device ID
            category: The category (e.g., "P", "M", "C", "S")
            index: The index (e.g., "T", "U", "P_int", etc.)
        
        Returns:
            Reading data dictionary, or None if not found
        """
        try:
            readings_key = f"device:{device_id}:category:{category}:readings"
            # Get the reading data directly from the index key
            reading_json = await self.redis_repository.redis_client.hget(readings_key, index)
            
            if not reading_json:
                logger.debug(f"No reading found for device {device_id}, category {category}, index {index}")
                return None
            
            try:
                reading = json.loads(reading_json)
                
                # Ensure we have a valid reading dictionary
                if not isinstance(reading, dict):
                    logger.warning(f"Invalid reading format for device {device_id}, category {category}, index {index}")
                    return None
                
                logger.debug(f"Retrieved reading for device {device_id}, category {category}, index {index}")
                return reading
                
            except json.JSONDecodeError:
                logger.error(f"Error parsing reading JSON for device {device_id}, category {category}, index {index}: {reading_json}")
                return None
            
        except Exception as e:
            logger.error(f"Error retrieving reading for device {device_id}, category {category}, index {index}: {str(e)}")
            return None
    

    async def get_device_readings_by_category(self, device_id: str, category: str) -> List[Dict[str, Any]]:
        """
        Get all latest readings (values, label, index, metadata) for a specific device and category.
        Returns a list of all reading data dictionaries for all indexes in the category.
        
        Args:
            device_id: The device ID
            category: The category (e.g., "P", "M", "C", "S")
        
        Returns:
            List of all reading data dictionaries for all indexes in the category
        """
        try:
            readings_key = f"device:{device_id}:category:{category}:readings"
            # Get all hash fields (index -> reading_data)
            reading_hash = await self.redis_repository.redis_client.hgetall(readings_key)
            
            if not reading_hash:
                logger.debug(f"No readings found for device {device_id}, category {category}")
                return []
            
            # Parse JSON strings and collect all readings
            all_readings = []
            for index, reading_json in reading_hash.items():
                try:
                    if reading_json:
                        reading_data = json.loads(reading_json)
                        
                        # Process the direct reading format
                        if isinstance(reading_data, dict):
                            all_readings.append(reading_data)
                        
                except json.JSONDecodeError:
                    logger.error(f"Error parsing reading JSON for index {index}: {reading_json}")
            
            # Sort readings by label for consistent ordering
            # all_readings.sort(key=lambda x: x.get('labels', [''])[0] if x.get('labels') else x.get('index', ''))
            
            logger.debug(f"Retrieved {len(all_readings)} total readings for device {device_id}, category {category}")
            return all_readings
            
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
    

    async def close(self) -> None:
        """
        Close the cache connection.
        """
        try:
            await self.redis_repository.close()
        except Exception as e:
            logger.error(f"Error closing cache connection: {str(e)}")
    
    async def get_cache_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the cached data.
        
        Returns:
            Dictionary containing cache statistics
        """
        try:
            stats = {}
            
            # Get all device IDs
            device_ids = await self.get_all_device_ids()
            stats['total_devices'] = len(device_ids)
            
            # Get category and index counts  
            category_counts = {}
            index_counts = {}
            label_counts = {}
            total_readings = 0
            
            for device_id in device_ids:
                categories = await self.get_device_categories(device_id)
                for category in categories:
                    if category not in category_counts:
                        category_counts[category] = 0
                    
                    # Get readings hash for this category
                    readings_key = f"device:{device_id}:category:{category}:readings"
                    reading_hash = await self.redis_repository.redis_client.hgetall(readings_key)
                    
                    for index, reading_json in reading_hash.items():
                        if index not in index_counts:
                            index_counts[index] = 0
                        index_counts[index] += 1
                        
                        try:
                            reading_data = json.loads(reading_json)
                            
                            # Process the direct reading format
                            if isinstance(reading_data, dict):
                                category_counts[category] += 1
                                total_readings += 1
                                
                                # Count by individual labels within the reading
                                labels = reading_data.get('label', [])
                                if isinstance(labels, list):
                                    for label in labels:
                                        if label not in label_counts:
                                            label_counts[label] = 0
                                        label_counts[label] += 1
                                    
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in reading for {device_id}:{category}:{index}")
            
            stats['total_readings'] = total_readings
            stats['categories'] = category_counts
            stats['indices'] = index_counts
            stats['labels'] = label_counts  # Now tracks individual sensor labels like T_ext, T_int, etc.
            stats['device_ids'] = device_ids
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting cache statistics: {str(e)}")
            return {} 