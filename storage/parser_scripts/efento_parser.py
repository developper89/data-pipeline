# storage/parser_scripts/coap_parser.py - Optimized Version with Reverse Alarm Parsing

import base64
import datetime
import json
import math
import logging
import os
import time
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger("parser_script")

# Constants
INDEX_MAP = {
    "MEASUREMENT_TYPE_TEMPERATURE": "T",
    "MEASUREMENT_TYPE_HUMIDITY": "U",
    "MEASUREMENT_TYPE_HUMIDITY_ACCURATE": "U",
    "MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE": "P",
    "MEASUREMENT_TYPE_CO2_GAS": "CO2",
    "MEASUREMENT_TYPE_CO_GAS": "CO",
    "MEASUREMENT_TYPE_NH3_GAS": "NH3",
    "MEASUREMENT_TYPE_CH4_GAS": "CH4",
    "MEASUREMENT_TYPE_NO2_GAS": "NO2",
}

BINARY_MEASUREMENT_TYPES = {
    "MEASUREMENT_TYPE_OK_ALARM",
    "MEASUREMENT_TYPE_FLOODING", 
    "MEASUREMENT_TYPE_OUTPUT_CONTROL",
    5, 7, 42  # Enum values
}

ACCUMULATOR_TYPES = {
    "MEASUREMENT_TYPE_PULSE_CNT_ACC_MAJOR",
    "MEASUREMENT_TYPE_WATER_METER_ACC_MAJOR",
    "MEASUREMENT_TYPE_ELEC_METER_ACC_MAJOR",
    "MEASUREMENT_TYPE_PULSE_CNT_ACC_MINOR",
    "MEASUREMENT_TYPE_WATER_METER_ACC_MINOR",
    "MEASUREMENT_TYPE_ELEC_METER_ACC_MINOR",
}

MAJOR_ACCUMULATOR_TYPES = {
    "MEASUREMENT_TYPE_PULSE_CNT_ACC_MAJOR",
    "MEASUREMENT_TYPE_WATER_METER_ACC_MAJOR",
    "MEASUREMENT_TYPE_ELEC_METER_ACC_MAJOR",
}

SCALING_CONFIGS = {
    "MEASUREMENT_TYPE_TEMPERATURE": {"scale": 10, "hysteresis": 1.0},
    "MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE": {"scale": 10, "hysteresis": 5.0},
    "MEASUREMENT_TYPE_HUMIDITY": {"scale": 1, "hysteresis": 2.0},
    "MEASUREMENT_TYPE_HUMIDITY_ACCURATE": {"scale": 10, "hysteresis": 2.0},
    "MEASUREMENT_TYPE_VOLTAGE": {"scale": 10, "hysteresis": 100.0},
    "MEASUREMENT_TYPE_CURRENT": {"scale": 100, "hysteresis": 10.0},
    "MEASUREMENT_TYPE_CURRENT_PRECISE": {"scale": 1000, "hysteresis": 10.0},
    "MEASUREMENT_TYPE_H2S_GAS": {"scale": 100, "hysteresis": None},
    "MEASUREMENT_TYPE_PERCENTAGE": {"scale": 100, "hysteresis": None},
    "MEASUREMENT_TYPE_AMBIENT_LIGHT": {"scale": 10, "hysteresis": None},
    "MEASUREMENT_TYPE_NOISE_LEVEL": {"scale": 10, "hysteresis": 2.0},
    "MEASUREMENT_TYPE_SOIL_MOISTURE": {"scale": 1, "hysteresis": None},
    "MEASUREMENT_TYPE_DISTANCE_MM": {"scale": 1, "hysteresis": None},
    "MEASUREMENT_TYPE_RESISTANCE": {"scale": 1, "hysteresis": None},
}

# Gas measurements with 1ppm resolution
GAS_TYPES = {
    "MEASUREMENT_TYPE_CO2_GAS", "MEASUREMENT_TYPE_CO_GAS",
    "MEASUREMENT_TYPE_NH3_GAS", "MEASUREMENT_TYPE_CH4_GAS", 
    "MEASUREMENT_TYPE_NO2_GAS"
}

# IAQ types with special scaling
IAQ_TYPES = {
    "MEASUREMENT_TYPE_IAQ", "MEASUREMENT_TYPE_STATIC_IAQ",
    "MEASUREMENT_TYPE_CO2_EQUIVALENT", "MEASUREMENT_TYPE_BREATH_VOC"
}

# Particulate matter types
PM_TYPES = {
    "MEASUREMENT_TYPE_PM_1_0", "MEASUREMENT_TYPE_PM_2_5", "MEASUREMENT_TYPE_PM_10_0"
}

# --- Utility Functions ---

def _decode_serial_number(serial_data: Any, fallback: str = "UNKNOWN_SERIAL_NUMBER") -> str:
    """
    Decode serial number from various formats to hex string.
    
    Args:
        serial_data: Serial number in various formats (base64 string, bytes, string, None)
        fallback: Fallback value if serial_data is None or empty
        
    Returns:
        Hex string representation of the serial number
    """
    if not serial_data:
        return fallback
    
    try:
        if isinstance(serial_data, bytes):
            # Raw bytes - convert directly to hex
            return serial_data.hex() if serial_data else fallback
        elif isinstance(serial_data, str):
            # Could be base64 encoded or plain string
            if serial_data:
                try:
                    # Try to decode as base64 first
                    decoded_bytes = base64.b64decode(serial_data)
                    return decoded_bytes.hex()
                except Exception:
                    # If base64 decode fails, treat as plain string
                    # Check if it's already a hex string
                    if all(c in '0123456789abcdefABCDEF' for c in serial_data):
                        return serial_data.lower()
                    else:
                        # Convert string to hex
                        return serial_data.encode('utf-8').hex()
            else:
                return fallback
        else:
            # Other types - convert to string then to hex
            return str(serial_data).encode('utf-8').hex()
    except Exception as e:
        logger.warning(f"Failed to decode serial number {serial_data}: {e}")
        return fallback

def _datetime_serializer(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def _get_device_info(decoded_data: Dict[str, Any], metadata: Dict[str, Any], serial_field: str = "serial_num") -> Tuple[str, str]:
    """Extract device ID and serial number from data and metadata."""
    device_id = metadata.get('device_id', 'UNKNOWN_DEVICE_ID')
    serial_data = decoded_data.get(serial_field, "")
    serial_num_hex = _decode_serial_number(serial_data)
    
    if serial_num_hex != device_id:
        logger.warning(f"Device ID mismatch: Config '{device_id}', Payload SN '{serial_num_hex}'")
    
    return device_id, serial_num_hex

def _create_base_output(device_id: str, serial_num_hex: str, battery_status: bool, 
                       channel_idx: int, **metadata_extra) -> Dict[str, Any]:
    """Create base output dictionary with common fields."""
    return {
        "device_id": device_id,
        "metadata": {
            "battery_ok": battery_status,
            "channel_number": channel_idx,
            "source_sn_hex": serial_num_hex,
            **metadata_extra
        }
    }

def _determine_sensor_type(measurement_types: List) -> str:
    """Determine sensor type from measurement types."""
    if not measurement_types:
        return "continuous"
    
    has_binary = any(mt in BINARY_MEASUREMENT_TYPES for mt in measurement_types if mt)
    has_continuous = any(mt not in BINARY_MEASUREMENT_TYPES for mt in measurement_types if mt)
    
    if has_binary and has_continuous:
        return "mixed"
    return "binary" if has_binary else "continuous"

def _calculate_measurement_period(base: int, factor: int, sensor_type: str = "continuous") -> int:
    """Calculate measurement period from base and factor."""
    if not (1 <= base <= 65535):
        raise ValueError(f"Base must be in range [1:65535], got {base}")
    
    # Handle backward compatibility
    if factor == 0:
        factor = 14 if sensor_type.lower() in ["binary", "mixed"] else 1
        logger.debug(f"Using backward compatibility factor {factor} for {sensor_type} sensor")
    
    if not (1 <= factor <= 65535):
        raise ValueError(f"Factor must be in range [1:65535], got {factor}")
    
    # Binary sensors ignore factor
    period = base if sensor_type.lower() == "binary" else base * factor
    logger.info(f"Calculated measurement period for {sensor_type} sensor: {period}s, base: {base}, factor: {factor}")
    return period

def _calculate_period_base_and_factor(period: int) -> Tuple[int, int]:
    """Calculate optimal base and factor for measurement period."""
    if period < 1:
        raise ValueError("Measurement period must be at least 1 second")
    if period > 65535 * 65535:
        raise ValueError(f"Measurement period too large: {period}s")
    
    # Try preferred bases for exact matches
    preferred_bases = [1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600]
    
    for base in preferred_bases:
        if period % base == 0:
            factor = period // base
            if 1 <= factor <= 65535:
                return base, factor
    
    # Find best approximation
    best_base, best_factor = 1, min(period, 65535)
    best_error = abs(period - (best_base * best_factor))
    
    for base in range(1, min(period + 1, 65536)):
        factor = period // base
        if not (1 <= factor <= 65535):
            continue
            
        actual_period = base * factor
        error = abs(period - actual_period)
        
        if error < best_error:
            best_base, best_factor = base, factor
            best_error = error
    
    return best_base, best_factor

def _get_scaling_config(measurement_type: str) -> Dict[str, Any]:
    """Get scaling configuration for measurement type."""
    if measurement_type in SCALING_CONFIGS:
        return SCALING_CONFIGS[measurement_type]
    elif measurement_type in GAS_TYPES:
        return {"scale": 1, "hysteresis": None}
    elif measurement_type in IAQ_TYPES:
        return {"scale": 3, "hysteresis": None}
    elif measurement_type in PM_TYPES:
        return {"scale": 1, "hysteresis": None}
    else:
        return {"scale": 1, "hysteresis": None}

def _encode_threshold_value(threshold: float, measurement_type_str: str, alarm_type: str) -> int:
    """Encode threshold value according to measurement type."""
    if alarm_type == 'Status' or measurement_type_str == 'MEASUREMENT_TYPE_OK_ALARM':
        return int(threshold) if threshold in [0, 1] else 1
    
    config = _get_scaling_config(measurement_type_str)
    return int(float(threshold) * config["scale"])

def _get_default_hysteresis(measurement_type_str: str, encoded_threshold: int) -> float:
    """Get default hysteresis value."""
    config = _get_scaling_config(measurement_type_str)
    if config["hysteresis"] is not None:
        return config["hysteresis"]
    # Default to 5% of threshold or minimum of 1
    return max(1.0, encoded_threshold * 0.05)

def _save_normalized_config_to_json(output_list: List[Dict[str, Any]], device_id: str, timestamp: datetime.datetime):
    """Save normalized config data to JSON file."""
    try:
        output_dir = "config_outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"normalized_config_{device_id}_{timestamp_str}.json"
        filepath = os.path.join(output_dir, filename)
        
        json_data = {
            "device_id": device_id,
            "timestamp": timestamp.isoformat(),
            "total_parameter_groups": len(output_list),
            "config_parameters": output_list
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, default=_datetime_serializer, ensure_ascii=False)
        
        logger.info(f"Saved normalized config data to: {filepath}")
        
    except Exception as e:
        logger.error(f"Failed to save normalized config to JSON: {e}")

# --- Normalization Functions ---

def _process_accumulator_channel(channel_data: Dict[str, Any], channel_idx: int, 
                                base_output: Dict[str, Any], measurement_period: int,
                                major_values: List[int], calibration_required: List[bool]) -> List[Dict[str, Any]]:
    """Process accumulator channels (major/minor)."""
    measure_type = channel_data["type"]
    timestamp_base = channel_data["timestamp"]
    start_point = channel_data["start_point"]
    sample_offsets = channel_data["sample_offsets"]
    
    output_list = []
    
    if measure_type in MAJOR_ACCUMULATOR_TYPES:
        # Process major accumulator
        temp_major_values = []
        temp_calibration_required = []
        
        for sample_offset in sample_offsets:
            meta_data = (start_point + sample_offset) % 4
            if "ELEC_METER" in measure_type or "PULSE_CNT" in measure_type:
                val = math.floor((start_point + sample_offset) / 4) * 1000
            else:  # Water Meter
                val = math.floor((start_point + sample_offset) / 4) * 100
            temp_major_values.append(val)
            temp_calibration_required.append(meta_data != 0)
        
        return temp_major_values, temp_calibration_required
    
    else:
        # Process minor accumulator
        if not major_values:
            logger.warning(f"Minor ACC channel {channel_idx} found without preceding Major values. Skipping.")
            return output_list
        
        output_index = measure_type.replace("MEASUREMENT_TYPE_", "").replace("_MINOR", "")
        
        for idx, sample_offset in enumerate(sample_offsets):
            if idx >= len(major_values):
                logger.warning(f"Index {idx} out of bounds for major values ({len(major_values)}) on channel {channel_idx}. Skipping offset.")
                continue
            
            if calibration_required[idx]:
                final_value = major_values[idx] + math.floor((start_point + sample_offset) / 6)
                value_metadata = {"calibration_required": True}
            else:
                final_value = major_values[idx] + ((start_point + sample_offset) // 6)
                value_metadata = {"calibration_required": False}
            
            reading_dt = datetime.datetime.fromtimestamp(timestamp_base + (measurement_period * idx), tz=datetime.timezone.utc)
            
            output_dict = {
                **base_output,
                "values": [final_value],
                "labels": None,
                "display_names": [f"CH{channel_idx} %name%"],
                "index": output_index,
                "timestamp": reading_dt,
                "metadata": {**base_output["metadata"], **value_metadata}
            }
            output_list.append(output_dict)
        
        return output_list

def _process_ok_alarm_channel(channel_data: Dict[str, Any], channel_idx: int, 
                             base_output: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process OK/Alarm channel."""
    timestamp_base = channel_data["timestamp"]
    sample_offsets = channel_data["sample_offsets"]
    
    change_points = {}
    for sample_offset in sample_offsets:
        time_at_change = timestamp_base + abs(sample_offset) - 1
        state = "Alarm" if sample_offset > 0 else "OK"
        change_points[time_at_change] = state
    
    output_list = []
    for t in sorted(change_points.keys()):
        state = change_points[t]
        reading_dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
        
        output_dict = {
            **base_output,
            "values": [state],
            "labels": None,
            "display_names": [f"CH{channel_idx} %name%"],
            "index": "OK_ALARM",
            "timestamp": reading_dt,
            "metadata": {**base_output["metadata"], "is_change_point": True}
        }
        output_list.append(output_dict)
    
    return output_list

def _process_standard_channel(channel_data: Dict[str, Any], channel_idx: int,
                             base_output: Dict[str, Any], measurement_period: int,
                             metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process standard numeric measurement channels."""
    measure_type = channel_data["type"]
    timestamp_base = channel_data["timestamp"]
    start_point = channel_data["start_point"]
    sample_offsets = channel_data["sample_offsets"]
    
    output_list = []
    
    for idx, sample_offset in enumerate(sample_offsets):
        if measure_type in ("MEASUREMENT_TYPE_TEMPERATURE", "MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE"):
            final_value = (start_point + sample_offset) / 10.0
        else:
            final_value = start_point + sample_offset
        
        reading_dt = datetime.datetime.fromtimestamp(timestamp_base + (measurement_period * idx), tz=datetime.timezone.utc)
        
        output_dict = {
            **base_output,
            "values": [final_value],
            "labels": [measure_type],
            "display_names": [f"CH{channel_idx} %name%"],
            "index": INDEX_MAP.get(measure_type, ""),
            "timestamp": reading_dt,
            "metadata": {**base_output["metadata"], **metadata}
        }
        output_list.append(output_dict)
    
    return output_list

def _normalize_measurements(decoded_data: Dict[str, Any], metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize decoded measurements data."""
    device_id, serial_num_hex = _get_device_info(decoded_data, metadata)
    
    battery_status = decoded_data.get("battery_status", False)
    measurement_period_base = decoded_data.get("measurement_period_base", 1)
    measurement_period_factor = decoded_data.get("measurement_period_factor", 1)
    channels = decoded_data.get("channels", [])
    
    # Determine sensor type and calculate measurement period
    channel_types = [ch.get("type", "") for ch in channels if ch]
    sensor_type = _determine_sensor_type(channel_types)
    measurement_period = _calculate_measurement_period(measurement_period_base, measurement_period_factor, sensor_type)
    
    output_list = []
    major_values = []
    calibration_required = []
    current_channel_idx_for_acc = 0
    
    for channel_idx, channel_data in enumerate(channels, start=1):
        if not channel_data or not isinstance(channel_data, dict):
            continue
        
        measure_type = channel_data.get("type", "")
        sample_offsets = channel_data.get("sample_offsets", [])
        
        if not measure_type or not sample_offsets:
            logger.warning(f"Skipping channel {channel_idx}: Missing type or sample_offsets")
            continue
        
        base_output = _create_base_output(device_id, serial_num_hex, battery_status, channel_idx)
        
        # Process different channel types
        if measure_type in MAJOR_ACCUMULATOR_TYPES:
            major_values, calibration_required = _process_accumulator_channel(
                channel_data, channel_idx, base_output, measurement_period, [], []
            )
            current_channel_idx_for_acc = channel_idx
            logger.debug(f"Processed Major values for channel {channel_idx}")
            
        elif measure_type in ACCUMULATOR_TYPES:  # Minor accumulators
            channel_outputs = _process_accumulator_channel(
                channel_data, channel_idx, base_output, measurement_period, major_values, calibration_required
            )
            output_list.extend(channel_outputs)
            major_values = []
            calibration_required = []
            
        elif measure_type == "MEASUREMENT_TYPE_OK_ALARM":
            channel_outputs = _process_ok_alarm_channel(channel_data, channel_idx, base_output)
            output_list.extend(channel_outputs)
            
        else:
            channel_outputs = _process_standard_channel(
                channel_data, channel_idx, base_output, measurement_period, metadata
            )
            output_list.extend(channel_outputs)
    
    return output_list

def _normalize_device_info(decoded_data: Dict[str, Any], metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize device info data."""
    device_id, source_sn_hex = _get_device_info(decoded_data, metadata, "serial_num")
    base_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    
    # Collect all parameters
    all_params = {}
    for param_name, value in decoded_data.items():
        if value is not None:
            if isinstance(value, bytes):
                all_params[param_name] = "disabled" if value == b'\x7f' else value.hex()
            else:
                all_params[param_name] = value
    
    return [{
        "device_id": device_id,
        "values": list(all_params.values()),
        "labels": list(all_params.keys()),
        "display_names": None,
        "index": "I",
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
        "metadata": {
            "source_sn_hex": source_sn_hex
        }
    }]

def _normalize_config(decoded_data: Dict[str, Any], metadata: Dict[str, Any], 
                     message_parser: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Normalize device config data."""
    device_id, source_sn_hex = _get_device_info(decoded_data, metadata, "serial_number")
    base_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    
    # Collect all parameters
    all_params = {}
    for param_name, value in decoded_data.items():
        if value is not None:
            if isinstance(value, bytes):
                all_params[param_name] = "disabled" if value == b'\x7f' else value.hex()
            else:
                all_params[param_name] = value
    
    # Add calculated measurement period
    channel_types = decoded_data.get("channel_types", [])
    sensor_type = _determine_sensor_type(channel_types) if channel_types else "continuous"
    all_params["measurement_period"] = _calculate_measurement_period(
        decoded_data.get("measurement_period_base", 0), 
        decoded_data.get("measurement_period_factor", 0), 
        sensor_type
    )
    
    return [{
        "device_id": device_id,
        "values": list(all_params.values()),
        "labels": list(all_params.keys()),
        "display_names": None,
        "index": "P",
        "timestamp": base_timestamp,
        "metadata": {
            "source_sn_hex": source_sn_hex,
            "config_hash": decoded_data.get("hash", 0),
            "hash_timestamp": decoded_data.get("hash_timestamp", 0),
            "error_timestamp": decoded_data.get("error_timestamp", 0),
            "data_type": "mixed",
            "parameter_count": len(all_params),
            "type_detection_method": "unified",
            "is_config_parameter": True,
            "alarms": [rule for rule in decoded_data.get("rules", []) if rule.get("condition", 0) != 1],
            "available_alarm_count": len([rule for rule in decoded_data.get("rules", []) if rule.get("condition", 0) == 1]),
        }
    }]

# --- Main Parse Function ---

def parse(payload: bytes, metadata: dict, config: dict, message_parser: Optional[Any] = None) -> Tuple[str, List[dict]]:
    """
    Parse raw CoAP payload using the protobuf message_parser and normalize output.
    
    Args:
        payload: Raw payload bytes
        metadata: Message metadata dictionary  
        config: Hardware config dictionary
        message_parser: ProtobufMessageParser instance
        
    Returns:
        List of standardized output dictionaries
    """
    logger.debug(f"CoAP parser received payload: {payload[:50]}...")
    
    if not message_parser:
        raise ValueError("Message parser instance is required for parsing")
    
    # Parse protobuf message
    try:
        parsed_message_type, decoded_data = message_parser.parse_message_to_dict(payload)
        if not parsed_message_type or not decoded_data:
            raise ValueError("Failed to parse protobuf message to dictionary")
        logger.debug(f"Successfully parsed {parsed_message_type} to dictionary")
    except Exception as e:
        logger.exception(f"Error using message_parser: {e}")
        raise RuntimeError(f"Protobuf parsing failed: {e}") from e
    
    # Normalize based on message type
    try:
        normalizers = {
            "measurements": _normalize_measurements,
            "device_info": _normalize_device_info,
            "config": lambda d, m, mp=None: _normalize_config(d, m, mp),
            "time": lambda d, m, mp=None: []  # No normalization needed
        }
        
        normalizer = normalizers.get(parsed_message_type)
        if not normalizer:
            raise RuntimeError(f"Decoder type '{parsed_message_type}' not handled in normalization.")
        
        if parsed_message_type == "config":
            normalized_output_list = normalizer(decoded_data, metadata, message_parser)
        else:
            normalized_output_list = normalizer(decoded_data, metadata)
            
        
    except Exception as e:
        logger.exception(f"Error during normalization of {parsed_message_type} data: {e}")
        raise RuntimeError(f"Normalization failed for {parsed_message_type}: {e}") from e
    
    logger.debug(f"Successfully parsed and normalized {len(normalized_output_list)} records.")
    return "data", normalized_output_list

# --- Command and Response Functions ---

def _get_proto_enum_value(module_name: str, enum_name: str, message_parser: Optional[Any]) -> int:
    """Get protobuf enum value safely."""
    if not message_parser or not hasattr(message_parser, 'compiler') or not message_parser.compiler:
        logger.error("message_parser or compiler not available")
        return 0
    
    module = message_parser.compiler.compiled_modules.get(module_name)
    if not module:
        logger.error(f"{module_name} module not found in compiled modules")
        return 0
    
    try:
        return getattr(module, enum_name)
    except AttributeError:
        logger.warning(f"Unknown enum: {enum_name}, defaulting to 0")
        return 0

def _calculate_efento_channel_mask(channel: int) -> int:
    """Convert 1-based channel number to Efento channel mask."""
    if not (1 <= channel <= 32):
        raise ValueError(f"Channel {channel} out of range (1-32)")
    return 1 << (channel - 1)

def _map_alarm_operator_to_condition(operator: str, message_parser: Optional[Any] = None) -> int:
    """Map alarm operator to Efento ProtoRule condition type."""
    mapping = {
        '>': 'CONDITION_HIGH_THRESHOLD', '>=': 'CONDITION_HIGH_THRESHOLD',
        '<': 'CONDITION_LOW_THRESHOLD', '<=': 'CONDITION_LOW_THRESHOLD',
        '==': 'CONDITION_BINARY_CHANGE_STATE', '!=': 'CONDITION_BINARY_CHANGE_STATE',
        'change': 'CONDITION_BINARY_CHANGE_STATE',
        'diff': 'CONDITION_DIFF_THRESHOLD', 'change_diff': 'CONDITION_DIFF_THRESHOLD'
    }
    
    condition_name = mapping.get(operator, 'CONDITION_HIGH_THRESHOLD')
    return _get_proto_enum_value('proto_rule_pb2', condition_name, message_parser)

def _map_alarm_level_to_action(level: str, message_parser: Optional[Any] = None) -> int:
    """Map alarm severity level to Efento ProtoRule action type."""
    mapping = {
        'critical': 'ACTION_TRIGGER_TRANSMISSION_WITH_ACK', 'high': 'ACTION_TRIGGER_TRANSMISSION_WITH_ACK',
        'warning': 'ACTION_TRIGGER_TRANSMISSION', 'medium': 'ACTION_TRIGGER_TRANSMISSION',
        'info': 'ACTION_FAST_ADVERTISING_MODE', 'low': 'ACTION_FAST_ADVERTISING_MODE',
        'debug': 'ACTION_NO_ACTION'
    }
    
    action_name = mapping.get(level.lower(), 'ACTION_TRIGGER_TRANSMISSION')
    return _get_proto_enum_value('proto_rule_pb2', action_name, message_parser)

def _encode_efento_alarm_parameters(alarm_data: dict, message_parser: Optional[Any] = None) -> List[int]:
    """Encode alarm threshold values according to Efento parameter format."""
    threshold = alarm_data['threshold']
    measurement_type_str = alarm_data['field_label']
    alarm_type = alarm_data.get('alarm_type', 'Measure')
    operator = alarm_data.get('math_operator', '>')
    
    # Get measurement type enum
    measurement_type_enum = _get_proto_enum_value('proto_measurement_types_pb2', measurement_type_str, message_parser)
    
    # Encode threshold and hysteresis
    encoded_threshold = _encode_threshold_value(threshold, measurement_type_str, alarm_type)
    hysteresis_value = alarm_data.get('hysteresis', _get_default_hysteresis(measurement_type_str, encoded_threshold))
    encoded_hysteresis = _encode_threshold_value(hysteresis_value, measurement_type_str, alarm_type)
    
    # Get triggering parameters
    triggering_mode = alarm_data.get('triggering_mode', 1)
    num_measurements = alarm_data.get('num_measurements', 1)
    
    # Build parameters based on condition type
    if operator in ['>', '>=', '<', '<=']:
        parameters = [encoded_threshold, encoded_hysteresis, triggering_mode, num_measurements, measurement_type_enum]
    elif operator in ['diff', 'change_diff']:
        parameters = [encoded_threshold, triggering_mode, num_measurements, measurement_type_enum]
    elif operator in ['==', '!=', 'change'] or alarm_type == 'Status':
        parameters = []
    else:
        parameters = [encoded_threshold, encoded_hysteresis, triggering_mode, num_measurements, measurement_type_enum]
    
    logger.debug(f"Encoded parameters for {measurement_type_str}: {parameters}")
    return parameters

# --- Reverse/Decode Functions ---

def _decode_efento_channel_mask(channel_mask: int) -> int:
    """Convert Efento channel mask to 1-based channel number. Inverse of _calculate_efento_channel_mask."""
    if channel_mask == 0:
        raise ValueError("Channel mask cannot be 0")
    
    # Find the position of the least significant bit set
    channel = 1
    mask = channel_mask
    while mask & 1 == 0:
        mask >>= 1
        channel += 1
    
    if channel > 32:
        raise ValueError(f"Channel {channel} out of range (1-32)")
    
    return channel

def _map_condition_to_alarm_operator(condition: int, message_parser: Optional[Any] = None) -> str:
    """Map Efento ProtoRule condition type to alarm operator. Inverse of _map_alarm_operator_to_condition."""
    # Create reverse mapping from the original function's mapping
    operator_to_condition = {
        '>': 'CONDITION_HIGH_THRESHOLD', '>=': 'CONDITION_HIGH_THRESHOLD',
        '<': 'CONDITION_LOW_THRESHOLD', '<=': 'CONDITION_LOW_THRESHOLD',
        '==': 'CONDITION_BINARY_CHANGE_STATE', '!=': 'CONDITION_BINARY_CHANGE_STATE',
        'change': 'CONDITION_BINARY_CHANGE_STATE',
        'diff': 'CONDITION_DIFF_THRESHOLD', 'change_diff': 'CONDITION_DIFF_THRESHOLD'
    }
    
    # Build reverse mapping by getting enum values
    condition_to_operator = {}
    for op, cond_name in operator_to_condition.items():
        enum_value = _get_proto_enum_value('proto_rule_pb2', cond_name, message_parser)
        if enum_value not in condition_to_operator:
            condition_to_operator[enum_value] = op
    
    # Add disabled condition
    disabled_enum = _get_proto_enum_value('proto_rule_pb2', 'CONDITION_DISABLED', message_parser)
    condition_to_operator[disabled_enum] = 'disabled'
    
    return condition_to_operator.get(condition, '>')

def _map_action_to_alarm_level(action: int, message_parser: Optional[Any] = None) -> str:
    """Map Efento ProtoRule action type to alarm severity level. Inverse of _map_alarm_level_to_action."""
    # Create reverse mapping from the original function's mapping
    level_to_action = {
        'critical': 'ACTION_TRIGGER_TRANSMISSION_WITH_ACK', 
        'high': 'ACTION_TRIGGER_TRANSMISSION_WITH_ACK',
        'warning': 'ACTION_TRIGGER_TRANSMISSION', 
        'medium': 'ACTION_TRIGGER_TRANSMISSION',
        'info': 'ACTION_FAST_ADVERTISING_MODE', 
        'low': 'ACTION_FAST_ADVERTISING_MODE',
        'debug': 'ACTION_NO_ACTION'
    }
    
    # Build reverse mapping by getting enum values
    action_to_level = {}
    for level, action_name in level_to_action.items():
        enum_value = _get_proto_enum_value('proto_rule_pb2', action_name, message_parser)
        if enum_value not in action_to_level:
            action_to_level[enum_value] = level
    
    return action_to_level.get(action, 'warning')

def _get_measurement_type_name_from_enum(measurement_type_enum: int, message_parser: Optional[Any] = None) -> str:
    """Get measurement type name from enum value. Uses proto module introspection."""
    if not message_parser or not hasattr(message_parser, 'compiler') or not message_parser.compiler:
        # Fallback mapping for common types
        type_mapping = {
            1: "MEASUREMENT_TYPE_TEMPERATURE",
            2: "MEASUREMENT_TYPE_HUMIDITY", 
            3: "MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE",
            5: "MEASUREMENT_TYPE_OK_ALARM",
            # Add more as needed
        }
        return type_mapping.get(measurement_type_enum, "MEASUREMENT_TYPE_TEMPERATURE")
    
    try:
        module = message_parser.compiler.compiled_modules.get('proto_measurement_types_pb2')
        if module:
            # Find the enum name by value
            for attr_name in dir(module):
                if attr_name.startswith('MEASUREMENT_TYPE_'):
                    if getattr(module, attr_name) == measurement_type_enum:
                        return attr_name
    except Exception as e:
        logger.warning(f"Failed to get measurement type from proto: {e}")
    
    return "MEASUREMENT_TYPE_TEMPERATURE"  # Default fallback

def _decode_threshold_value(encoded_threshold: int, measurement_type_str: str, alarm_type: str = "Measure") -> float:
    """Decode threshold value according to measurement type. Inverse of _encode_threshold_value."""
    if alarm_type == 'Status' or measurement_type_str == 'MEASUREMENT_TYPE_OK_ALARM':
        return float(encoded_threshold)
    
    config = _get_scaling_config(measurement_type_str)
    return float(encoded_threshold) / config["scale"]

def parse_alarms(decoded_rules: List[Dict[str, Any]], message_parser: Optional[Any] = None) -> List[Dict[str, Any]]:
    """
    Parse decoded Efento alarm rules back to JSON payload format.
    
    This function is the inverse of _format_efento_alarm_command() and reuses the same
    helper functions where possible by creating inverse mappings.
    
    Args:
        decoded_rules: List of decoded rule dictionaries from protobuf parsing
        message_parser: ProtobufMessageParser instance
        
    Returns:
        List of dictionaries with alarm configurations in JSON format
        
    Raises:
        ValueError: If no rules provided or invalid rule format
    """
    logger.debug(f"Parsing {len(decoded_rules)} Efento alarm rules")
    
    if not decoded_rules:
        raise ValueError("No alarm rules provided")
    
    parsed_alarms = []
    
    for rule_idx, rule_data in enumerate(decoded_rules):
        try:
            # Extract basic rule information using inverse helper functions
            channel_mask = rule_data.get('channel_mask', 0)
            condition = rule_data.get('condition', 0)
            action = rule_data.get('action', 0)
            parameters = rule_data.get('parameters', [])
            
            if channel_mask == 0:
                logger.warning(f"Skipping rule {rule_idx}: Invalid channel_mask")
                continue
            
            channel_number = _decode_efento_channel_mask(channel_mask)  # Inverse of _calculate_efento_channel_mask
            math_operator = _map_condition_to_alarm_operator(condition, message_parser)  # Inverse of _map_alarm_operator_to_condition
            level = _map_action_to_alarm_level(action, message_parser)  # Inverse of _map_alarm_level_to_action
            
            # Check if alarm is disabled (reuses _get_proto_enum_value logic)
            active = math_operator != 'disabled'
            if not active:
                math_operator = '>'  # Default for disabled alarms
            
            # Parse parameters based on condition type (mirrors _encode_efento_alarm_parameters logic)
            alarm_payload = {
                "active": active,
                "level": level,
                "math_operator": math_operator,
                "alarm_type": "Measure"  # Default
            }
        
            # Extract measurement type and threshold based on parameter structure
            # This mirrors the logic from _encode_efento_alarm_parameters
            if math_operator in ['>', '>=', '<', '<='] and len(parameters) >= 5:
                # Standard threshold alarm: [threshold, hysteresis, triggering_mode, num_measurements, measurement_type]
                encoded_threshold = parameters[0]
                encoded_hysteresis = parameters[1]
                triggering_mode = parameters[2]
                num_measurements = parameters[3]
                measurement_type_enum = parameters[4]
                
                measurement_type_str = _get_measurement_type_name_from_enum(measurement_type_enum, message_parser)
                threshold = _decode_threshold_value(encoded_threshold, measurement_type_str)
                hysteresis = _decode_threshold_value(encoded_hysteresis, measurement_type_str)
                
                alarm_payload.update({
                    "threshold": threshold,
                    "field_label": measurement_type_str,
                    "hysteresis": hysteresis,
                    "triggering_mode": triggering_mode,
                    "num_measurements": num_measurements
                })
                
            elif math_operator in ['diff', 'change_diff'] and len(parameters) >= 4:
                # Difference threshold: [threshold, triggering_mode, num_measurements, measurement_type]
                encoded_threshold = parameters[0]
                triggering_mode = parameters[1]
                num_measurements = parameters[2]
                measurement_type_enum = parameters[3]
                
                measurement_type_str = _get_measurement_type_name_from_enum(measurement_type_enum, message_parser)
                threshold = _decode_threshold_value(encoded_threshold, measurement_type_str)
                
                alarm_payload.update({
                    "threshold": threshold,
                    "field_label": measurement_type_str,
                    "triggering_mode": triggering_mode,
                    "num_measurements": num_measurements
                })
                
            elif math_operator in ['==', '!=', 'change']:
                # Binary/status change - no parameters needed (mirrors the empty parameters case)
                alarm_payload.update({
                    "threshold": 1,  # Default binary threshold
                    "field_label": "MEASUREMENT_TYPE_OK_ALARM",
                    "alarm_type": "Status"
                })
                
            else:
                # Unknown or malformed rule - use default values like _encode_efento_alarm_parameters fallback
                logger.warning(f"Unknown alarm rule format: condition={condition}, parameters={parameters}")
                alarm_payload.update({
                    "threshold": 0,
                    "field_label": "MEASUREMENT_TYPE_TEMPERATURE"
                })
            
            # Build complete alarm configuration
            alarm_config = {
                "command_type": "alarm",
                "device_id": "UNKNOWN_DEVICE_ID",  # Not available in rule data
                "payload": alarm_payload,
                "metadata": {
                    "channel_number": channel_number,
                    "device_type": "efento",
                    "parsed_timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                    "rule_index": rule_idx,
                    "original_condition": condition,
                    "original_action": action,
                    "parameter_count": len(parameters)
                }
            }
            
            parsed_alarms.append(alarm_config)
            logger.debug(f"Successfully parsed alarm rule {rule_idx}: {alarm_payload}")
            
        except Exception as e:
            logger.error(f"Failed to parse alarm rule {rule_idx}: {e}")
            # Continue processing other rules instead of failing completely
            continue
    
    logger.info(f"Successfully parsed {len(parsed_alarms)} out of {len(decoded_rules)} alarm rules")
    return parsed_alarms

# --- Original Command Formatting Functions ---

def format_command(command: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """Format commands for Efento devices using protobuf message_parser."""
    logger.info(f"Formatting Efento command: {command.get('command_type')} for device {command.get('device_id')}")
    
    command_type = command.get('command_type')
    payload = command.get('payload', {})
    
    formatters = {
        "alarm": lambda: _format_efento_alarm_command(payload, command.get('metadata', {}), config, message_parser),
        "config": lambda: _format_efento_config_command(payload, config, message_parser),
        "refresh_config": lambda: _format_efento_refresh_config_command(message_parser),
        "firmware_update": lambda: _format_efento_firmware_command(payload, config, message_parser)
    }
    
    formatter = formatters.get(command_type)
    if not formatter:
        raise ValueError(f"Unsupported command type for Efento device: {command_type}")
    
    return formatter()

def _format_efento_alarm_command(alarm_data: dict, metadata: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """Convert alarm configuration to Efento ProtoConfig with embedded ProtoRule."""
    logger.debug(f"Creating Efento alarm rule: {alarm_data}")
    
    # Extract channel and create rule
    channel = metadata.get('channel_number', 1)
    alarm_params = alarm_data.copy()
    alarm_params['math_operator'] = alarm_data['math_operator']
    
    efento_rule = {
        'channel_mask': _calculate_efento_channel_mask(channel),
        'condition': _map_alarm_operator_to_condition(alarm_data['math_operator'], message_parser),
        'parameters': _encode_efento_alarm_parameters(alarm_params, message_parser),
        'action': _map_alarm_level_to_action(alarm_data['level'], message_parser),
    }
    
    # Disable rule if not active
    if not alarm_data.get('active', True):
        efento_rule['condition'] = _get_proto_enum_value('proto_rule_pb2', 'CONDITION_DISABLED', message_parser)
    
    # Create ProtoConfig
    proto_config_data = {
        'current_time': int(time.time()),
        'rules': [efento_rule],
        'hash': int(time.time()),
        'hash_timestamp': int(time.time())
    }
    
    logger.debug(f"Generated ProtoConfig structure: {proto_config_data}")
    
    # Serialize protobuf message
    try:
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()
        
        # Set basic fields
        proto_message.current_time = proto_config_data['current_time']
        proto_message.request_device_info = True
        proto_message.ack_interval = 0xFFFFFFFF
        
        # Add rules
        for rule_dict in proto_config_data['rules']:
            rule_msg = proto_message.rules.add()
            rule_msg.channel_mask = rule_dict['channel_mask']
            rule_msg.condition = rule_dict['condition']
            rule_msg.action = rule_dict['action']
            rule_msg.parameters.extend(rule_dict['parameters'])
        
        binary_config = proto_message.SerializeToString()
        if not binary_config:
            raise RuntimeError("Failed to serialize ProtoConfig")
        
        logger.debug(f"Successfully encoded Efento alarm command: {len(binary_config)} bytes")
        return binary_config
        
    except Exception as e:
        logger.error(f"Failed to encode Efento alarm command: {e}")
        raise RuntimeError(f"Protobuf encoding failed: {e}") from e

def _format_efento_config_command(config_data: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """Format device configuration command to Efento ProtoConfig."""
    logger.debug(f"Creating Efento config command: {config_data}")
    
    # Build base config
    proto_config_data = {
        'request_device_info': True,
        'current_time': int(time.time()),
    }
    
    # Handle measurement periods
    if 'measurement_period' in config_data:
        base, factor = _calculate_period_base_and_factor(config_data['measurement_period'])
        proto_config_data['measurement_period_base'] = base
        proto_config_data['measurement_period_factor'] = factor
        logger.info(f"Measurement period: {config_data['measurement_period']}, base: {base}, factor: {factor}")
    
    # Add standard config fields (excluding special ones)
    special_fields = {
        'measurement_period', 'dns_server_ip', 'cellular_config_params', 'led_config', 
        'encryption_key', 'ble_advertising_period', 'network_search', 
        'output_control_state_request', 'calibration_parameters_request', 'calendars'
    }
    
    for key, value in config_data.items():
        if key not in special_fields:
            proto_config_data[key] = value
    
    # Handle special fields
    _handle_special_config_fields(config_data, proto_config_data)
    
    # Serialize protobuf
    try:
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()
        
        # Set simple fields
        for field_name, value in proto_config_data.items():
            if hasattr(proto_message, field_name):
                _set_proto_field(proto_message, field_name, value)
        
        # Handle complex nested fields
        _handle_complex_config_fields(config_data, proto_message)
        
        binary_config = proto_message.SerializeToString()
        if not binary_config:
            raise RuntimeError("Failed to serialize ProtoConfig")

        logger.debug(f"Successfully encoded Efento config command: {len(binary_config)} bytes")
        return binary_config
        
    except Exception as e:
        logger.error(f"Failed to encode Efento config command: {e}")
        raise RuntimeError(f"Config encoding failed: {e}") from e

def _handle_special_config_fields(config_data: dict, proto_config_data: dict):
    """Handle special configuration fields that need processing."""
    # DNS Server IP
    if 'dns_server_ip' in config_data:
        dns_ip = config_data['dns_server_ip']
        if isinstance(dns_ip, str):
            proto_config_data['dns_server_ip'] = [int(octet) for octet in dns_ip.split('.')]
        elif isinstance(dns_ip, list):
            proto_config_data['dns_server_ip'] = dns_ip
    
    # List fields
    for field in ['cellular_config_params', 'led_config']:
        if field in config_data and isinstance(config_data[field], list):
            proto_config_data[field] = config_data[field]
    
    # Encryption key
    if 'encryption_key' in config_data:
        value = config_data['encryption_key']
        if isinstance(value, str):
            if value.lower() == 'disabled' or value == '\x7f':
                proto_config_data['encryption_key'] = b'\x7f'
            else:
                try:
                    proto_config_data['encryption_key'] = bytes.fromhex(value)
                except ValueError:
                    proto_config_data['encryption_key'] = value.encode('utf-8')
        elif isinstance(value, bytes):
            proto_config_data['encryption_key'] = value

def _set_proto_field(proto_message, field_name: str, value):
    """Set protobuf field value handling repeated fields."""
    field_descriptor = proto_message.DESCRIPTOR.fields_by_name.get(field_name)
    if field_descriptor:
        if field_descriptor.label == field_descriptor.LABEL_REPEATED:
            if isinstance(value, list):
                getattr(proto_message, field_name).extend(value)
            else:
                getattr(proto_message, field_name).append(value)
        else:
            setattr(proto_message, field_name, value)

def _handle_complex_config_fields(config_data: dict, proto_message):
    """Handle complex nested configuration fields."""
    # BLE Advertising Period
    if 'ble_advertising_period' in config_data:
        ble_config = config_data['ble_advertising_period']
        if isinstance(ble_config, dict):
            ble_msg = proto_message.ble_advertising_period
            for key in ['mode', 'normal', 'fast']:
                if key in ble_config:
                    setattr(ble_msg, key, ble_config[key])
    
    # Network Search Configuration
    if 'network_search' in config_data:
        network_config = config_data['network_search']
        if isinstance(network_config, dict):
            network_msg = proto_message.network_search
            for key, value in network_config.items():
                if hasattr(network_msg, key):
                    if isinstance(value, list):
                        getattr(network_msg, key).extend(value)
                    else:
                        setattr(network_msg, key, value)
    
    # Output Control State Request
    if 'output_control_state_request' in config_data:
        output_states = config_data['output_control_state_request']
        if isinstance(output_states, list):
            for state_dict in output_states:
                if isinstance(state_dict, dict):
                    state_msg = proto_message.output_control_state_request.add()
                    for key, value in state_dict.items():
                        if hasattr(state_msg, key):
                            setattr(state_msg, key, value)
    
    # Calibration Parameters Request
    if 'calibration_parameters_request' in config_data:
        cal_config = config_data['calibration_parameters_request']
        if isinstance(cal_config, dict):
            cal_msg = proto_message.calibration_parameters_request
            for key, value in cal_config.items():
                if hasattr(cal_msg, key):
                    if key == 'parameters' and isinstance(value, list):
                        cal_msg.parameters.extend(value)
                    else:
                        setattr(cal_msg, key, value)

def _format_efento_refresh_config_command(message_parser: Optional[Any] = None) -> bytes:
    """Format configuration refresh command to request current device configuration."""
    
    try:
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()
        
        # Set required fields for configuration refresh request
        current_time = int(time.time())
        proto_message.current_time = current_time
        proto_message.request_device_info = True
        proto_message.request_configuration = True


        # Set acknowledgment interval if provided
        # ack_interval = refresh_data.get('ack_interval', 0xFFFFFFFF)
        # proto_message.ack_interval = ack_interval
        
        # Serialize the protobuf message
        binary_config = proto_message.SerializeToString()
        if not binary_config:
            raise RuntimeError("Failed to serialize ProtoConfig refresh request")
        
        logger.debug(f"Successfully encoded Efento refresh config command: {len(binary_config)} bytes")
        return binary_config
        
    except Exception as e:
        logger.error(f"Failed to encode Efento refresh config command: {e}")
        raise RuntimeError(f"Refresh config encoding failed: {e}") from e

def _format_efento_firmware_command(firmware_data: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """Format firmware update command (placeholder)."""
    logger.warning("Firmware update commands not yet implemented for Efento devices")
    raise NotImplementedError("Efento firmware updates not implemented")

def format_response(config: dict, message_parser: Optional[Any] = None) -> bytes:
    """Generate Efento-specific response payload for CoAP communication."""
    try:
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()
        
        # Set required fields for Efento response
        current_time = int(time.time())
        proto_message.current_time = current_time
        proto_message.request_device_info = True
        # proto_message.ack_interval = 0xFFFFFFFF
        
        response_bytes = proto_message.SerializeToString()
        if not response_bytes:
            raise RuntimeError("Failed to serialize Efento response")
        
        logger.info(f"Generated Efento response: {len(response_bytes)} bytes, current_time: {current_time}")
        return response_bytes
        
    except Exception as e:
        logger.error(f"Failed to generate Efento response: {e}")
        raise RuntimeError(f"Efento response generation failed: {e}") from e