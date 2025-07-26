# normalizer/cache_context.py
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class CacheContext:
    """
    Provides cache access interface for parsers.
    
    This class allows parser scripts to query current cached readings
    when they need to preserve existing state during partial updates.
    """
    
    def __init__(self, sensor_cache_service):
        """
        Initialize cache context with sensor cache service.
        
        Args:
            sensor_cache_service: Instance of SensorCacheService from preservarium_sdk
        """
        self.sensor_cache_service = sensor_cache_service
    
    async def get_current_reading(self, device_id: str, category: str) -> Optional[Dict[str, Any]]:
        """
        Get current cached reading for device/category.
        
        Args:
            device_id: Device identifier
            category: Reading category (e.g., 'L' for light controller)
            
        Returns:
            Current cached reading dict or None if not found
        """
        try:
            # Use the sensor cache service to get readings by category
            readings = await self.sensor_cache_service.get_device_readings_by_category(
                device_id=device_id, 
                category=category, 
                read_only=False  # We need full data, not just read-only
            )
            
            if readings and len(readings) > 0:
                # Return the first (most recent) reading
                current_reading = readings[0]
                logger.debug(f"Retrieved cached reading for device {device_id} category {category}")
                return current_reading
            else:
                logger.debug(f"No cached reading found for device {device_id} category {category}")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to get cached reading for {device_id}/{category}: {e}")
            return None
    
    async def reading_exists(self, device_id: str, category: str) -> bool:
        """
        Check if cached reading exists for device/category.
        
        Args:
            device_id: Device identifier  
            category: Reading category
            
        Returns:
            True if reading exists, False otherwise
        """
        reading = await self.get_current_reading(device_id, category)
        return reading is not None
    
    async def get_device_categories(self, device_id: str) -> list:
        """
        Get all available categories for a device.
        
        Args:
            device_id: Device identifier
            
        Returns:
            List of available categories
        """
        try:
            categories = await self.sensor_cache_service.get_device_categories(device_id)
            logger.debug(f"Device {device_id} has categories: {categories}")
            return categories
        except Exception as e:
            logger.warning(f"Failed to get categories for device {device_id}: {e}")
            return []