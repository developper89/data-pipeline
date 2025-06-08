import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
import time

# Import the ValidatedOutput model
from shared.models.common import ValidatedOutput

logger = logging.getLogger("ingestor-influxdb3.data_mapper")

class DataMapper:
    """
    Maps normalized data from Kafka to InfluxDB3 line protocol format.
    Handles field mappings, tags, and measurements for InfluxDB3.
    """
    
    def __init__(self):
        pass
    
    def _escape_measurement_name(self, name: str) -> str:
        """
        Escape measurement name for line protocol.
        Measurement names with spaces or special characters need to be escaped.
        """
        # Replace spaces with underscores and remove other problematic characters
        escaped = name.replace(" ", "_").replace(",", "_").replace("=", "_")
        return escaped
    
    def _escape_tag_value(self, value: str) -> str:
        """
        Escape tag values for line protocol.
        Tag values need commas, spaces, and equals signs escaped.
        """
        if not isinstance(value, str):
            value = str(value)
        
        # Escape special characters in tag values
        return value.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")
    
    def _escape_field_key(self, key: str) -> str:
        """
        Escape field keys for line protocol.
        Field keys need commas, spaces, and equals signs escaped.
        """
        return key.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")
    
    def _format_field_value(self, value: Any) -> str:
        """
        Format field value for line protocol.
        Different data types have different formatting requirements.
        """
        if isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, int):
            return f"{value}i"  # Integer values need 'i' suffix
        elif isinstance(value, float):
            return str(value)   # Float values as-is
        elif isinstance(value, str):
            # String values need to be quoted and escaped
            escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
            return f'"{escaped}"'
        else:
            # Convert other types to string
            escaped = str(value).replace("\\", "\\\\").replace("\"", "\\\"")
            return f'"{escaped}"'
    
    def _convert_timestamp(self, timestamp: datetime, precision: str = "s") -> int:
        """
        Convert datetime to timestamp in the specified precision.
        InfluxDB3 supports ns, us, ms, s precision.
        """
        epoch_seconds = timestamp.timestamp()
        
        if precision == "ns":
            return int(epoch_seconds * 1_000_000_000)
        elif precision == "us":
            return int(epoch_seconds * 1_000_000)
        elif precision == "ms":
            return int(epoch_seconds * 1_000)
        else:  # "s"
            return int(epoch_seconds)
        
    def map_data(self, validated_data: ValidatedOutput, precision: str = "s") -> str:
        """
        Maps a ValidatedOutput message to InfluxDB3 line protocol format.
        
        Args:
            validated_data: ValidatedOutput object from Kafka
            precision: Timestamp precision (ns, us, ms, s)
            
        Returns:
            String in line protocol format, or empty string if no valid data
        """
        try:
            # Get essential data from the validated output
            device_id = validated_data.device_id
            values = validated_data.values
            labels = validated_data.label or []
            timestamp = validated_data.timestamp
            metadata = validated_data.metadata or {}
            
            # Skip invalid messages
            if not values:
                logger.warning("Invalid message: missing values")
                return ""
                
            # Use device_id as the measurement name (escaped)
            measurement = self._escape_measurement_name(device_id)
            
            # Prepare tags - we can use device_id and any metadata as tags
            tags = []
            tags.append(f"device_id={self._escape_tag_value(device_id)}")
            
            # Add metadata as tags if they're simple string/number values
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)) and key not in ["persist"]:
                    tag_key = self._escape_field_key(str(key))
                    tag_value = self._escape_tag_value(str(value))
                    tags.append(f"{tag_key}={tag_value}")
            
            tag_set = ",".join(tags) if tags else ""
            
            # Prepare fields (actual measurement values)
            fields = []
            
            # Process the values
            for i, value in enumerate(values):
                field_name = labels[i] if i < len(labels) else f"value_{i}"
                field_key = self._escape_field_key(field_name)
                field_value = self._format_field_value(value)
                fields.append(f"{field_key}={field_value}")
                    
            # Skip if no fields
            if not fields:
                logger.warning(f"No valid fields found in message for device {device_id}")
                return ""
                
            field_set = ",".join(fields)
            
            # Convert timestamp
            timestamp_ns = self._convert_timestamp(timestamp, precision)
            
            # Build line protocol string
            # Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
            if tag_set:
                line_protocol = f"{measurement},{tag_set} {field_set} {timestamp_ns}"
            else:
                line_protocol = f"{measurement} {field_set} {timestamp_ns}"
            
            logger.debug(f"Generated line protocol: {line_protocol}")
            return line_protocol
            
        except Exception as e:
            logger.error(f"Error mapping data to line protocol: {str(e)}")
            return ""
    
    def map_batch_data(self, validated_data_list: List[ValidatedOutput], precision: str = "s") -> str:
        """
        Maps multiple ValidatedOutput messages to InfluxDB3 line protocol format.
        
        Args:
            validated_data_list: List of ValidatedOutput objects from Kafka
            precision: Timestamp precision (ns, us, ms, s)
            
        Returns:
            String with multiple lines in line protocol format
        """
        lines = []
        for validated_data in validated_data_list:
            line = self.map_data(validated_data, precision)
            if line:
                lines.append(line)
        
        return "\n".join(lines) 