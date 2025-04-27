import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Union

# Import the ValidatedOutput model
from shared.models.common import ValidatedOutput

logger = logging.getLogger("ingestor.data_mapper")

class DataMapper:
    """
    Maps normalized data from Kafka to InfluxDB data points.
    Handles field mappings, tags, and measurements.
    """
    
    def __init__(self):
        pass
        
    def map_data(self, validated_data: ValidatedOutput) -> List[Dict[str, Any]]:
        """
        Maps a ValidatedOutput message to InfluxDB data points.
        
        Args:
            validated_data: ValidatedOutput object from Kafka
            
        Returns:
            List of dictionaries formatted for InfluxDB ingestion
        """
        try:
            # Get essential data from the validated output
            device_id = validated_data.device_id
            values = validated_data.values
            timestamp = validated_data.timestamp
            
            # Skip invalid messages
            if not values:
                logger.warning("Invalid message: missing values")
                return []
                
            # Use device_id as the measurement name
            measurement = device_id
            
            # Prepare fields (actual measurement values)
            fields = {}
            
            # Process the values
            for i, value in enumerate(values):
                field_name = validated_data.label[i]
                if isinstance(value, (int, float)):
                    fields[field_name] = value
                elif isinstance(value, str):
                    try:
                        # Try to convert string to number if possible
                        fields[field_name] = float(value)
                    except ValueError:
                        # Keep string values as is
                        fields[field_name] = value
                    
            # Skip if no fields
            if not fields:
                logger.warning(f"No valid fields found in message for device {device_id}")
                return []
                
            # Create InfluxDB point
            point = {
                "measurement": measurement,
                "fields": fields,
                "time": timestamp
            }
            
            return [point]
            
        except Exception as e:
            logger.error(f"Error mapping data: {str(e)}")
            return [] 