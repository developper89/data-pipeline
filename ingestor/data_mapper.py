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
            
            # Get persist flags from metadata
            metadata = validated_data.metadata or {}
            persist_flags = metadata.get("persist", [])
            
            # Process the values - only include values that should be persisted
            for i, value in enumerate(values):
                # Check if this value should be persisted (persist_flags is always a list)
                should_persist = True  # Default to True if no flag specified
                if i < len(persist_flags):
                    should_persist = persist_flags[i]
                
                # Skip values that shouldn't be persisted
                if not should_persist:
                    logger.debug(f"Skipping value at index {i} for device {device_id} (persist=False)")
                    continue
                
                # Get field name from labels
                field_name = None
                if validated_data.labels and i < len(validated_data.labels):
                    field_name = validated_data.labels[i]
                else:
                    field_name = f"value_{i}"  # Fallback field name
                
                # Add value to fields if it should be persisted
                if isinstance(value, (int, float)):
                    fields[field_name] = value
                elif isinstance(value, str):
                    try:
                        # Try to convert string to number if possible
                        fields[field_name] = float(value)
                    except ValueError:
                        # Keep string values as is
                        fields[field_name] = value
                else:
                    # Handle other data types (bool, etc.)
                    fields[field_name] = value
                    
            # Skip if no fields (either no values or all values have persist=False)
            if not fields:
                # Check if it's because all values were filtered out by persistence
                has_non_persistent_values = any(not flag for flag in persist_flags[:len(values)]) if persist_flags else False
                    
                if has_non_persistent_values:
                    logger.info(f"No fields created for device {device_id} - all values have persist=False")
                else:
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