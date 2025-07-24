# storage/parser_scripts/coap_parser.py Andy

import base64
import datetime
import json
import math
import logging
import os
import time
from typing import List, Dict, Any, Optional

# Configure optional logging within the script
logger = logging.getLogger("parser_script")

index_map = {
    "MEASUREMENT_TYPE_TEMPERATURE": "T",
    "MEASUREMENT_TYPE_HUMIDITY": "U",
}
# --- Helper Functions ---

def _datetime_serializer(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def _save_normalized_config_to_json(output_list: List[Dict[str, Any]], device_id: str, timestamp: datetime.datetime):
    """
    Save normalized config data to a JSON file.
    
    Args:
        output_list: The normalized config output list
        device_id: Device identifier for the filename
        timestamp: Timestamp for the filename
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = "config_outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with device_id and timestamp
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"normalized_config_{device_id}_{timestamp_str}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Prepare data for JSON serialization
        json_data = {
            "device_id": device_id,
            "timestamp": timestamp.isoformat(),
            "total_parameter_groups": len(output_list),
            "config_parameters": output_list
        }
        
        # Write to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, default=_datetime_serializer, ensure_ascii=False)
        
        logger.info(f"Saved normalized config data to: {filepath}")
        
    except Exception as e:
        logger.error(f"Failed to save normalized config to JSON: {e}")

def _normalize_measurements(decoded_data: Dict[str, Any], metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalization logic for decoded measurements data.
    Creates a list of dictionaries matching StandardizedOutput structure.
    """
    output_list = []
    
    # Get device_id from config metadata (raw message metadata)
    # The message_parser instance doesn't contain the extracted device_id, so get it from config
    device_id = metadata.get('device_id', 'UNKNOWN_DEVICE_ID')
    
    # Get additional metadata from config (raw message metadata) and message_parser
    base_labels = None  # CoAP doesn't typically have predefined labels

    # Extract top-level info from decoded protobuf data
    serial_num_b64 = decoded_data.get("serial_num", "")
    serial_num_hex = base64.b64decode(serial_num_b64).hex() if serial_num_b64 else device_id
    
    if serial_num_hex != device_id:
        logger.warning(f"Device ID mismatch: Config '{device_id}', Payload SN '{serial_num_hex}'")

    battery_status = decoded_data.get("battery_status", False)
    measurement_period_base = decoded_data.get("measurement_period_base", 1)
    measurement_period_factor = decoded_data.get("measurement_period_factor", 1)
    
    # Determine sensor type from channels to calculate measurement period correctly
    channels = decoded_data.get("channels", [])
    sensor_type = _determine_sensor_type_from_channels(channels)
    measurement_period = _calculate_measurement_period(measurement_period_base, measurement_period_factor, sensor_type)
    major_values = []
    calibration_required = []
    current_channel_idx_for_acc = 0

    for channel_idx, channel_data in enumerate(channels, start=1):
        if not channel_data or not isinstance(channel_data, dict):
            continue

        measure_type = channel_data.get("type", "")
        timestamp_base = channel_data.get("timestamp", 0)
        start_point = channel_data.get("start_point", 0)
        sample_offsets = channel_data.get("sample_offsets", [])

        if not measure_type or not sample_offsets:
             logger.warning(f"Skipping channel {channel_idx}: Missing type or sample_offsets")
             continue

        # --- Pre-process Major Counters ---
        if measure_type in (
            "MEASUREMENT_TYPE_PULSE_CNT_ACC_MAJOR",
            "MEASUREMENT_TYPE_WATER_METER_ACC_MAJOR",
            "MEASUREMENT_TYPE_ELEC_METER_ACC_MAJOR",
        ):
            temp_major_values = []
            temp_calibration_required = []
            for sample_offset in sample_offsets:
                meta_data = (start_point + sample_offset) % 4
                if "ELEC_METER" in measure_type or "PULSE_CNT" in measure_type:
                    val = math.floor((start_point + sample_offset) / 4) * 1000
                else: # Water Meter
                    val = math.floor((start_point + sample_offset) / 4) * 100
                temp_major_values.append(val)
                temp_calibration_required.append(meta_data != 0)
            
            major_values = temp_major_values
            calibration_required = temp_calibration_required
            current_channel_idx_for_acc = channel_idx
            logger.debug(f"Processed Major values for channel {channel_idx}")

        # --- Process Minor Accumulators ---
        elif measure_type in (
            "MEASUREMENT_TYPE_PULSE_CNT_ACC_MINOR",
            "MEASUREMENT_TYPE_WATER_METER_ACC_MINOR",
            "MEASUREMENT_TYPE_ELEC_METER_ACC_MINOR",
        ):
            if not major_values or channel_idx <= current_channel_idx_for_acc:
                 logger.warning(f"Minor ACC channel {channel_idx} found without preceding Major values. Skipping.")
                 continue

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
                      "device_id": device_id,
                      "values": [final_value],
                      "labels": base_labels,
                      "display_names": [f"CH{channel_idx} %name%"],
                      "index": output_index,
                      "timestamp": reading_dt,
                      "metadata": {
                           "battery_ok": battery_status,
                           "channel_number": channel_idx,
                           "source_sn_hex": serial_num_hex,
                           **value_metadata
                      }
                 }
                 output_list.append(output_dict)
            
            major_values = []
            calibration_required = []

        # --- Process OK/Alarm ---
        elif measure_type == "MEASUREMENT_TYPE_OK_ALARM":
            change_points = {}
            for sample_offset in sample_offsets:
                time_at_change = timestamp_base + abs(sample_offset) - 1
                state = "Alarm" if sample_offset > 0 else "OK"
                change_points[time_at_change] = state

            sorted_times = sorted(change_points.keys())
            for t in sorted_times:
                state = change_points[t]
                reading_dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
                output_dict = {
                     "device_id": device_id,
                     "values": [state],
                     "labels": base_labels,
                     "display_names": [f"CH{channel_idx} %name%"],
                     "index": measure_type.replace("MEASUREMENT_TYPE_", ""),
                     "timestamp": reading_dt,
                     "metadata": {
                          "battery_ok": battery_status,
                          "channel_number": channel_idx,
                          "source_sn_hex": serial_num_hex,
                          "is_change_point": True
                     }
                }
                output_list.append(output_dict)

        # --- Process Standard Numeric Types ---
        else:
            logger.debug(f"Processing sample_offset: {sample_offsets}")
            for idx, sample_offset in enumerate(sample_offsets):
                if measure_type in ("MEASUREMENT_TYPE_TEMPERATURE", "MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE"):
                     final_value = (start_point + sample_offset) / 10.0
                else:
                     final_value = start_point + sample_offset

                reading_dt = datetime.datetime.fromtimestamp(timestamp_base + (measurement_period * idx), tz=datetime.timezone.utc)

                output_dict = {
                     "device_id": device_id,
                     "values": [final_value],
                     "labels": [measure_type],
                     "display_names": [f"CH{channel_idx} %name%"],
                     "index": index_map.get(measure_type, ""),
                     "timestamp": reading_dt,
                     "metadata": {
                          "battery_ok": battery_status,
                          "channel_number": channel_idx,
                          "source_sn_hex": serial_num_hex,
                          **metadata
                     }
                }
                output_list.append(output_dict)

    return output_list


def _normalize_device_info(decoded_data: Dict[str, Any], metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalizes device info into a single StandardizedOutput dictionary.
    """
    # Get device_id from config metadata (raw message metadata)
    device_id = metadata.get('device_id', 'UNKNOWN_DEVICE_ID')

    serial_num_b64 = decoded_data.get("serial_num", "")
    serial_num_hex = base64.b64decode(serial_num_b64).hex() if serial_num_b64 else device_id

    output_dict = {
        "device_id": device_id,
        "values": ["device_info_received"],
        "labels": None,
        "display_names": None,
        "index": "DEVICE_INFO",
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
        "metadata": {
            "source_sn_hex": serial_num_hex,
            "full_device_info": decoded_data
        }
    }
    return [output_dict]


def _normalize_config(decoded_data: Dict[str, Any], metadata: Dict[str, Any], message_parser: Optional[Any] = None) -> List[Dict[str, Any]]:
    """
    Normalizes device config confirmation into a single StandardizedOutput dictionary.
    All parameters are combined into one output regardless of their data type.
    """
    # Get device_id from config metadata (raw message metadata)
    device_id = metadata.get('device_id', 'UNKNOWN_DEVICE_ID')
    base_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    
    # Extract common metadata
    serial_number = decoded_data.get("serial_number", b"")
    if isinstance(serial_number, bytes):
        source_sn_hex = serial_number.hex() if serial_number else "UNKNOWN_SERIAL_NUMBER"
    else:
        source_sn_hex = str(serial_number) if serial_number else "UNKNOWN_SERIAL_NUMBER"
    
    config_hash = decoded_data.get("hash", 0)
    hash_timestamp = decoded_data.get("hash_timestamp", 0)
    error_timestamp = decoded_data.get("error_timestamp", 0)
    
    # Collect all parameters into a single combined dictionary
    all_params = {}
    for param_name, value in decoded_data.items():
        if value is not None:
            # Handle bytes values specially to make them readable
            if isinstance(value, bytes):
                if value == b'\x7f':  # Special case for disabled encryption
                    all_params[param_name] = "disabled"
                else:
                    all_params[param_name] = value.hex()
            else:
                all_params[param_name] = value
    # Determine sensor type from channel types if available
    channel_types = decoded_data.get("channel_types", [])
    sensor_type = _determine_overall_sensor_type(channel_types) if channel_types else "continuous"
    
    all_params["measurement_period"] = _calculate_measurement_period(
        decoded_data.get("measurement_period_base", 0), 
        decoded_data.get("measurement_period_factor", 0), 
        sensor_type
    )
    # Create a single output dictionary with all parameters
    output_dict = {
        "device_id": device_id,
        "values": list(all_params.values()),
        "labels": list(all_params.keys()),
        "display_names": None,
        "index": "P",
        "timestamp": base_timestamp,
        "metadata": {
            "source_sn_hex": source_sn_hex,
            "config_hash": config_hash,
            "hash_timestamp": hash_timestamp,
            "error_timestamp": error_timestamp,
            "data_type": "mixed",
            "parameter_count": len(all_params),
            "type_detection_method": "unified",
            "is_config_parameter": True
        }
    }
    
    return [output_dict]








# --- MANDATORY PARSE FUNCTION ---

def parse(payload: bytes, metadata: dict, config: dict, message_parser: Optional[Any] = None) -> List[dict]:
    """
    Parses raw CoAP payload using the protobuf message_parser,
    and normalizes it into a list of dictionaries matching the StandardizedOutput structure.

    Args:
        payload: bytes of the raw payload.
        message_parser: The ProtobufMessageParser instance initialized by the normalizer
                   based on message_parser_used metadata.
        metadata: The raw message metadata dictionary containing CoAP-specific
                info like 'uri_path', 'method', 'source', etc.
        config: The raw message config dictionary containing Hardware-specific
                config
    Returns:
        A list of dictionaries formatted for StandardizedOutput validation.

    Raises:
        ValueError: If input types are wrong, decoding fails, path is missing/unknown,
                    or normalization fails.
        Exception: For other unexpected errors.
    """
    logger.debug(f"CoAP parser received payload: {payload[:50]}...")
    logger.debug(f"CoAP parser received message_parser: {message_parser}")
    logger.debug(f"CoAP parser received metadata: {metadata}")

    # --- 2. Validate message_parser instance ---
    if not message_parser:
        raise ValueError("Message parser instance is required for parsing")

    # Use the message_parser's message_parser to parse the protobuf data to dictionary
    try:
        logger.debug(f"Using message_parser's message_parser to parse protobuf message to dictionary")
        
        # Use the message_parser's parse_message_to_dict method for automatic detection, parsing and conversion
        parsed_message_type, decoded_data = message_parser.parse_message_to_dict(payload)
        
        if not parsed_message_type or not decoded_data:
            raise ValueError(f"Failed to parse protobuf message to dictionary")
        
        decoder_type = parsed_message_type
        
        logger.debug(f"Successfully parsed {parsed_message_type} to dictionary using message_parser's message_parser")

    except Exception as e:
        logger.exception(f"Error using message_parser: {e}")
        raise RuntimeError(f"Protobuf parsing failed using message_parser: {e}") from e

    # --- 3. Normalize Decoded Data based on Type ---
    normalized_output_list: List[Dict[str, Any]] = []

    try:
        if decoder_type == "measurements":
            normalized_output_list = _normalize_measurements(decoded_data, metadata)
            logger.info(f"Normalized measurements: {normalized_output_list}")
        elif decoder_type == "device_info":
             normalized_output_list = _normalize_device_info(decoded_data, metadata)
             logger.info(f"Normalized device info: {normalized_output_list}")
        elif decoder_type == "config":
             normalized_output_list = _normalize_config(decoded_data, metadata, message_parser)
             logger.info(f"Normalized config: {normalized_output_list}")
        elif decoder_type == "time":
            # No normalization needed for time requests
            normalized_output_list = []
        else:
             raise RuntimeError(f"Decoder type '{decoder_type}' not handled in normalization.")

    except Exception as e:
         logger.exception(f"Error during normalization of {decoder_type} data: {e}")
         raise RuntimeError(f"Normalization failed for {decoder_type}: {e}") from e

    logger.info(f"Successfully parsed and normalized {len(normalized_output_list)} records.")
    return normalized_output_list


# --- COMMAND FORMATTING FUNCTION ---

def format_command(command: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """
    Format commands for Efento devices using protobuf message_parser.

    Args:
        command: Standardized command dictionary with:
            - command_type: "alarm", "config", "firmware_update", etc.
            - device_id: Target device identifier
            - payload: Command-specific data dictionary
            - metadata: Additional command context
        message_parser: ProtobufMessageParser instance for Efento
        config: Hardware configuration dictionary

    Returns:
        bytes: Protobuf-encoded ProtoConfig message ready for CoAP transmission

    Raises:
        ValueError: If command_type is unsupported
        RuntimeError: If protobuf encoding fails
    """
    logger.info(f"Formatting Efento command: {command.get('command_type')} for device {command.get('device_id')}")

    command_type = command.get('command_type')
    payload = command.get('payload', {})
    device_id = command.get('device_id')
    metadata = command.get('metadata', {})

    if command_type == "alarm":
        return _format_efento_alarm_command(payload, metadata, config, message_parser)
    elif command_type == "config":
        return _format_efento_config_command(payload, config, message_parser)
    elif command_type == "firmware_update":
        return _format_efento_firmware_command(payload, config, message_parser)
    else:
        raise ValueError(f"Unsupported command type for Efento device: {command_type}")

def _calculate_efento_channel_mask(channel: int) -> int:
    """
    Convert 1-based channel number to Efento channel mask.

    Efento uses bit masks where:
    - Channel 1 = bit 0 (mask = 1)
    - Channel 2 = bit 1 (mask = 2)
    - Channel 3 = bit 2 (mask = 4)
    - etc.

    Args:
        channel: 1-based channel number

    Returns:
        int: Channel mask value
    """
    if channel < 1 or channel > 32:
        raise ValueError(f"Channel {channel} out of range (1-32)")

    return 1 << (channel - 1)


def _encode_efento_alarm_parameters(alarm_data: dict, message_parser: Optional[Any] = None) -> List[int]:
    """
    Encode alarm threshold values according to Efento parameter format.

    For threshold conditions (HIGH/LOW), parameters are:
    - parameter[0]: Threshold value (encoded according to measurement type)
    - parameter[1]: Hysteresis value (encoded according to measurement type)
    - parameter[2]: Triggering mode (1=moving avg, 2=window avg, 3=consecutive)
    - parameter[3]: Number of measurements for trigger determination (1-10)
    - parameter[4]: Measurement type (MeasurementType enum value)

    For differential threshold conditions:
    - parameter[0]: Threshold delta value
    - parameter[1]: Triggering mode
    - parameter[2]: Number of measurements
    - parameter[3]: Measurement type

    Args:
        alarm_data: Alarm configuration with threshold, measurement_type, alarm_type, etc.

    Returns:
        List[int]: Encoded parameter array for ProtoRule
    """
    threshold = alarm_data['threshold']
    measurement_type_str = alarm_data['field_label']  # String name like 'MEASUREMENT_TYPE_TEMPERATURE'
    alarm_type = alarm_data.get('alarm_type', 'Measure')
    operator = alarm_data.get('math_operator', '>')
    
    # Get the actual enum value from the measurement type string
    measurement_type_enum = _get_measurement_type_enum(measurement_type_str, message_parser)
    
    # Encode the threshold value based on measurement type
    encoded_threshold = _encode_threshold_value(threshold, measurement_type_str, alarm_type)
    
    # Default hysteresis (can be configurable)
    hysteresis_value = alarm_data.get('hysteresis', _get_default_hysteresis(measurement_type_str, encoded_threshold))
    encoded_hysteresis = _encode_threshold_value(hysteresis_value, measurement_type_str, alarm_type)
    
    # Triggering mode (default to moving average)
    triggering_mode = alarm_data.get('triggering_mode', 1)  # 1=moving avg, 2=window avg, 3=consecutive
    
    # Number of measurements for trigger determination
    num_measurements = alarm_data.get('num_measurements', 1)  # Default to single measurement
    
    # Build parameters based on condition type
    if operator in ['>', '>=', '<', '<=']:
        # HIGH_THRESHOLD or LOW_THRESHOLD condition
        parameters = [
            encoded_threshold,      # parameter[0]: Threshold value
            encoded_hysteresis,     # parameter[1]: Hysteresis value  
            triggering_mode,        # parameter[2]: Triggering mode
            num_measurements,       # parameter[3]: Number of measurements
            measurement_type_enum   # parameter[4]: Measurement type enum
        ]
    elif operator in ['diff', 'change_diff']:
        # DIFF_THRESHOLD condition
        parameters = [
            encoded_threshold,      # parameter[0]: Threshold delta value
            triggering_mode,        # parameter[1]: Triggering mode
            num_measurements,       # parameter[2]: Number of measurements
            measurement_type_enum   # parameter[3]: Measurement type enum
        ]
    elif operator in ['==', '!=', 'change'] or alarm_type == 'Status':
        # BINARY_CHANGE_STATE condition - no parameters needed
        parameters = []
    else:
        # Default case - use threshold parameters
        parameters = [
            encoded_threshold,
            encoded_hysteresis,
            triggering_mode,
            num_measurements,
            measurement_type_enum
        ]
    
    logger.debug(f"Encoded parameters for {measurement_type_str}: {parameters}")
    return parameters


def _get_measurement_type_enum(measurement_type_str: str, message_parser: Optional[Any] = None) -> int:
    """
    Convert measurement type string to enum value.
    
    Args:
        measurement_type_str: String like 'MEASUREMENT_TYPE_TEMPERATURE'
        message_parser: ProtobufMessageParser instance for accessing compiled modules
        
    Returns:
        int: Enum value from proto_measurement_types_pb2
    """
    # Get proto_measurement_types_pb2 from the message_parser's compiled modules
    if message_parser and hasattr(message_parser, 'compiler') and message_parser.compiler:
        proto_measurement_types_pb2 = message_parser.compiler.compiled_modules.get('proto_measurement_types_pb2')
        if not proto_measurement_types_pb2:
            logger.error("proto_measurement_types_pb2 module not found in compiled modules")
            return 0  # NO_SENSOR
    else:
        logger.error("message_parser or compiler not available")
        return 0  # NO_SENSOR
    
    # Get the enum value
    try:
        enum_value = getattr(proto_measurement_types_pb2, measurement_type_str)
        logger.debug(f"Mapped {measurement_type_str} to enum value {enum_value}")
        return enum_value
    except AttributeError:
        logger.warning(f"Unknown measurement type: {measurement_type_str}, defaulting to 0")
        return 0  # NO_SENSOR


def _encode_threshold_value(threshold: float, measurement_type_str: str, alarm_type: str) -> int:
    """
    Encode threshold value according to measurement type resolution.
    
    Args:
        threshold: Raw threshold value
        measurement_type_str: Measurement type string
        alarm_type: Alarm type (Status or Measure)
        
    Returns:
        int: Encoded threshold value
    """
    # For Status alarms or OK_ALARM measurement type, threshold is typically 0 or 1
    if alarm_type == 'Status' or measurement_type_str == 'MEASUREMENT_TYPE_OK_ALARM':
        encoded_value = int(threshold) if threshold in [0, 1] else 1
    elif measurement_type_str == 'MEASUREMENT_TYPE_TEMPERATURE':
        # Temperature: multiply by 10 (Resolution 0.1°C)
        encoded_value = int(float(threshold) * 10)
    elif measurement_type_str in ['MEASUREMENT_TYPE_HUMIDITY', 'MEASUREMENT_TYPE_HUMIDITY_ACCURATE']:
        # Humidity: use as-is for HUMIDITY (1%), multiply by 10 for HUMIDITY_ACCURATE (0.1%)
        if measurement_type_str == 'MEASUREMENT_TYPE_HUMIDITY_ACCURATE':
            encoded_value = int(float(threshold) * 10)
        else:
            encoded_value = int(float(threshold))
    elif measurement_type_str == 'MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE':
        # Atmospheric pressure: multiply by 10 (Resolution 0.1hPa)
        encoded_value = int(float(threshold) * 10)
    elif measurement_type_str == 'MEASUREMENT_TYPE_DIFFERENTIAL_PRESSURE':
        # Differential pressure: use as-is (Resolution 1Pa)
        encoded_value = int(float(threshold))
    elif measurement_type_str == 'MEASUREMENT_TYPE_HIGH_PRESSURE':
        # High pressure: use as-is (Resolution 1kPa)
        encoded_value = int(float(threshold))
    elif measurement_type_str == 'MEASUREMENT_TYPE_VOLTAGE':
        # Voltage: multiply by 10 (Resolution 0.1mV)
        encoded_value = int(float(threshold) * 10)
    elif measurement_type_str == 'MEASUREMENT_TYPE_CURRENT':
        # Current: multiply by 100 (Resolution 0.01mA)
        encoded_value = int(float(threshold) * 100)
    elif measurement_type_str == 'MEASUREMENT_TYPE_CURRENT_PRECISE':
        # Current precise: multiply by 1000 (Resolution 0.001mA)
        encoded_value = int(float(threshold) * 1000)
    elif measurement_type_str == 'MEASUREMENT_TYPE_H2S_GAS':
        # H2S gas: multiply by 100 (Resolution 0.01ppm)
        encoded_value = int(float(threshold) * 100)
    elif measurement_type_str in ['MEASUREMENT_TYPE_CO2_GAS', 'MEASUREMENT_TYPE_CO_GAS', 
                                  'MEASUREMENT_TYPE_NH3_GAS', 'MEASUREMENT_TYPE_CH4_GAS', 
                                  'MEASUREMENT_TYPE_NO2_GAS']:
        # Gas concentrations: use as-is (Resolution 1ppm)
        encoded_value = int(float(threshold))
    elif measurement_type_str == 'MEASUREMENT_TYPE_PERCENTAGE':
        # Percentage: multiply by 100 (Resolution 0.01%)
        encoded_value = int(float(threshold) * 100)
    elif measurement_type_str == 'MEASUREMENT_TYPE_AMBIENT_LIGHT':
        # Ambient light: multiply by 10 (Resolution 0.1lx)
        encoded_value = int(float(threshold) * 10)
    elif measurement_type_str == 'MEASUREMENT_TYPE_NOISE_LEVEL':
        # Noise level: multiply by 10 (Resolution 0.1dB)
        encoded_value = int(float(threshold) * 10)
    elif measurement_type_str == 'MEASUREMENT_TYPE_SOIL_MOISTURE':
        # Soil moisture: use as-is (Resolution 1kPa)
        encoded_value = int(float(threshold))
    elif measurement_type_str == 'MEASUREMENT_TYPE_DISTANCE_MM':
        # Distance: use as-is (Resolution 1mm)
        encoded_value = int(float(threshold))
    elif measurement_type_str in ['MEASUREMENT_TYPE_PULSE_CNT', 'MEASUREMENT_TYPE_PULSE_CNT_ACC_MINOR', 
                                  'MEASUREMENT_TYPE_PULSE_CNT_ACC_MAJOR', 'MEASUREMENT_TYPE_PULSE_CNT_ACC_WIDE_MINOR',
                                  'MEASUREMENT_TYPE_PULSE_CNT_ACC_WIDE_MAJOR']:
        # Pulse counters: use as-is (Resolution 1 pulse)
        encoded_value = int(float(threshold))
    elif measurement_type_str in ['MEASUREMENT_TYPE_WATER_METER', 'MEASUREMENT_TYPE_WATER_METER_ACC_MINOR',
                                  'MEASUREMENT_TYPE_WATER_METER_ACC_MAJOR']:
        # Water meter: use as-is (Resolution 1l for minor, 1hl for major)
        encoded_value = int(float(threshold))
    elif measurement_type_str in ['MEASUREMENT_TYPE_ELECTRICITY_METER', 'MEASUREMENT_TYPE_ELEC_METER_ACC_MINOR',
                                  'MEASUREMENT_TYPE_ELEC_METER_ACC_MAJOR']:
        # Electricity meter: use as-is (Resolution 1Wh for minor, 1kWh for major)
        encoded_value = int(float(threshold))
    elif measurement_type_str in ['MEASUREMENT_TYPE_PM_1_0', 'MEASUREMENT_TYPE_PM_2_5', 'MEASUREMENT_TYPE_PM_10_0']:
        # Particulate matter: use as-is (Resolution 1µg/m³)
        encoded_value = int(float(threshold))
    elif measurement_type_str in ['MEASUREMENT_TYPE_IAQ', 'MEASUREMENT_TYPE_STATIC_IAQ', 
                                  'MEASUREMENT_TYPE_CO2_EQUIVALENT', 'MEASUREMENT_TYPE_BREATH_VOC']:
        # IAQ-related: multiply by 3 (values are divided by 3 to get calibration status)
        encoded_value = int(float(threshold) * 3)
    elif measurement_type_str == 'MEASUREMENT_TYPE_RESISTANCE':
        # Resistance: use as-is (Resolution 1Ohm)
        encoded_value = int(float(threshold))
    else:
        # Default: no scaling for unknown measurement types
        encoded_value = int(float(threshold))
        logger.debug(f"Using raw threshold value for unknown measurement type '{measurement_type_str}'")

    logger.debug(f"Encoded threshold {threshold} ({measurement_type_str}) → {encoded_value}")
    return encoded_value


def _get_default_hysteresis(measurement_type_str: str, encoded_threshold: int) -> float:
    """
    Get default hysteresis value based on measurement type and threshold.
    
    Args:
        measurement_type_str: Measurement type string
        encoded_threshold: Encoded threshold value
        
    Returns:
        float: Default hysteresis value (raw, not encoded)
    """
    if measurement_type_str == 'MEASUREMENT_TYPE_TEMPERATURE':
        return 1.0  # 1°C default hysteresis
    elif measurement_type_str in ['MEASUREMENT_TYPE_HUMIDITY', 'MEASUREMENT_TYPE_HUMIDITY_ACCURATE']:
        return 2.0  # 2% default hysteresis  
    elif measurement_type_str == 'MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE':
        return 5.0  # 5hPa default hysteresis
    elif measurement_type_str == 'MEASUREMENT_TYPE_VOLTAGE':
        return max(100.0, encoded_threshold * 0.02)  # 100mV or 2% of threshold
    elif measurement_type_str in ['MEASUREMENT_TYPE_CURRENT', 'MEASUREMENT_TYPE_CURRENT_PRECISE']:
        return max(10.0, encoded_threshold * 0.05)  # 10mA or 5% of threshold
    elif measurement_type_str in ['MEASUREMENT_TYPE_CO2_GAS', 'MEASUREMENT_TYPE_CO_GAS']:
        return max(50, encoded_threshold * 0.05)  # 50ppm or 5% of threshold
    elif measurement_type_str == 'MEASUREMENT_TYPE_NOISE_LEVEL':
        return 2.0  # 2dB default hysteresis
    elif measurement_type_str == 'MEASUREMENT_TYPE_AMBIENT_LIGHT':
        return max(10.0, encoded_threshold * 0.1)  # 10lx or 10% of threshold
    else:
        # Default to 5% of threshold or minimum of 1
        return max(1.0, encoded_threshold * 0.05)


def _map_alarm_operator_to_condition(operator: str, message_parser: Optional[Any] = None) -> int:
    """
    Map alarm operator to Efento ProtoRule condition type.
    Updated to handle all operators correctly.
    """
    # Get proto_rule_pb2 from the message_parser's compiled modules
    if message_parser and hasattr(message_parser, 'compiler') and message_parser.compiler:
        proto_rule_pb2 = message_parser.compiler.compiled_modules.get('proto_rule_pb2')
        if not proto_rule_pb2:
            logger.error("proto_rule_pb2 module not found in compiled modules")
            return 2  # Default to CONDITION_HIGH_THRESHOLD
    else:
        logger.error("message_parser or compiler not available")
        return 2  # Default to CONDITION_HIGH_THRESHOLD
    
    condition_mapping = {
        '>': proto_rule_pb2.CONDITION_HIGH_THRESHOLD,
        '>=': proto_rule_pb2.CONDITION_HIGH_THRESHOLD,
        '<': proto_rule_pb2.CONDITION_LOW_THRESHOLD,
        '<=': proto_rule_pb2.CONDITION_LOW_THRESHOLD,
        '==': proto_rule_pb2.CONDITION_BINARY_CHANGE_STATE,  # For binary sensors
        '!=': proto_rule_pb2.CONDITION_BINARY_CHANGE_STATE,  # For binary sensors
        'change': proto_rule_pb2.CONDITION_BINARY_CHANGE_STATE,
        'diff': proto_rule_pb2.CONDITION_DIFF_THRESHOLD,
        'change_diff': proto_rule_pb2.CONDITION_DIFF_THRESHOLD
    }

    condition = condition_mapping.get(operator)
    if condition is None:
        logger.warning(f"Unknown operator '{operator}', defaulting to CONDITION_HIGH_THRESHOLD")
        return proto_rule_pb2.CONDITION_HIGH_THRESHOLD

    logger.debug(f"Mapped operator '{operator}' to condition {condition}")
    return condition

def _map_alarm_level_to_action(level: str, message_parser: Optional[Any] = None) -> int:
    """
    Map alarm severity level to Efento ProtoRule action type.

    Args:
        level: Alarm severity level from AlarmMessage (CRITICAL, WARNING, INFO)
        message_parser: ProtobufMessageParser instance for accessing compiled modules

    Returns:
        int: Efento action enum value from proto_rule.proto
    """
    # Get proto_rule_pb2 from the message_parser's compiled modules
    if message_parser and hasattr(message_parser, 'compiler') and message_parser.compiler:
        proto_rule_pb2 = message_parser.compiler.compiled_modules.get('proto_rule_pb2')
        if not proto_rule_pb2:
            logger.error("proto_rule_pb2 module not found in compiled modules")
            return 1  # Default to ACTION_TRIGGER_TRANSMISSION
    else:
        logger.error("message_parser or compiler not available")
        return 1  # Default to ACTION_TRIGGER_TRANSMISSION
    
    # Map to Efento Action enum values from proto_rule.proto
    action_mapping = {
        'critical': proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION_WITH_ACK,  # ACTION_TRIGGER_TRANSMISSION_WITH_ACK - Requires acknowledgment
        'high': proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION_WITH_ACK,      # ACTION_TRIGGER_TRANSMISSION_WITH_ACK
        'warning': proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION,   # ACTION_TRIGGER_TRANSMISSION - Standard transmission
        'medium': proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION,    # ACTION_TRIGGER_TRANSMISSION
        'info': proto_rule_pb2.ACTION_FAST_ADVERTISING_MODE,      # ACTION_FAST_ADVERTISING_MODE - Faster BLE advertising
        'low': proto_rule_pb2.ACTION_FAST_ADVERTISING_MODE,       # ACTION_FAST_ADVERTISING_MODE
        'debug': proto_rule_pb2.ACTION_NO_ACTION      # ACTION_NO_ACTION - For testing
    }

    action = action_mapping.get(level.lower(), proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION)  # Default to ACTION_TRIGGER_TRANSMISSION
    logger.debug(f"Mapped alarm level '{level}' → action {action}")

    return action


# Updated main function to pass the operator to parameter encoding
def _format_efento_alarm_command(alarm_data: dict, metadata: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """
    Convert alarm configuration to Efento ProtoConfig with embedded ProtoRule.
    Updated to include all required parameters.
    """
    logger.debug(f"Creating Efento alarm rule: {alarm_data}")

    # Validate AlarmMessage data
    # _validate_alarm_message_data(alarm_data)

    # Extract channel number from metadata
    channel = metadata.get('channel_number', 1)  # Default to channel 1
    
    # Extract measurement type from field_name and field_label

    # Add operator to alarm data for parameter encoding
    alarm_params = alarm_data.copy()
    alarm_params['math_operator'] = alarm_data['math_operator']

    # Create Efento-specific rule structure
    efento_rule = {
        'channel_mask': _calculate_efento_channel_mask(channel),
        'condition': _map_alarm_operator_to_condition(alarm_data['math_operator'], message_parser),
        'parameters': _encode_efento_alarm_parameters(alarm_params, message_parser),
        'action': _map_alarm_level_to_action(alarm_data['level'], message_parser),
    }

    # If the alarm is not active, disable the rule
    if not alarm_data.get('active', True):
        # Get proto_rule_pb2 from the message_parser's compiled modules
        if message_parser and hasattr(message_parser, 'compiler') and message_parser.compiler:
            proto_rule_pb2 = message_parser.compiler.compiled_modules.get('proto_rule_pb2')
            if proto_rule_pb2:
                efento_rule['condition'] = proto_rule_pb2.CONDITION_DISABLED
            else:
                efento_rule['condition'] = 0  # Default to CONDITION_DISABLED

    # Create ProtoConfig structure with the rule
    proto_config_data = {
        'request_configuration': True,
        'current_time': int(time.time()),
        'rules': [efento_rule],
        'hash': int(time.time()),
        'hash_timestamp': int(time.time())
    }

    logger.debug(f"Generated ProtoConfig structure: {proto_config_data}")

    # Create the actual protobuf message
    try:
        from google.protobuf.json_format import MessageToDict
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()

        # Set basic fields
        proto_message.current_time = proto_config_data['current_time']
        proto_message.request_configuration = proto_config_data['request_configuration']
        proto_message.request_device_info = True
        proto_message.ack_interval = 0xFFFFFFFF

        # Handle rules array
        for rule_dict in proto_config_data['rules']:
            rule_msg = proto_message.rules.add()
            rule_msg.channel_mask = rule_dict['channel_mask']
            rule_msg.condition = rule_dict['condition']
            rule_msg.action = rule_dict['action']

            # Set parameters array
            for param in rule_dict['parameters']:
                rule_msg.parameters.append(param)
                
        proto_dict = MessageToDict(proto_message)
        logger.info(f"ProtoConfig as dict: {json.dumps(proto_dict, indent=2)}")
        
        # Serialize to bytes
        binary_config = proto_message.SerializeToString()

        if not binary_config:
            raise RuntimeError("Failed to serialize ProtoConfig")

        logger.info(f"Successfully encoded Efento alarm command: {len(binary_config)} bytes")
        return binary_config

    except Exception as e:
        logger.error(f"Failed to encode Efento alarm command: {e}")
        raise RuntimeError(f"Protobuf encoding failed: {e}") from e

def _get_sensor_type_from_measurement_type(measurement_type) -> str:
    """
    Determine sensor type (continuous/binary) from measurement type string or enum value.
    
    Args:
        measurement_type: Measurement type string (e.g., "MEASUREMENT_TYPE_TEMPERATURE") or enum value
        
    Returns:
        str: Sensor type ("continuous" or "binary")
    """
    # Binary sensor types (from proto_measurement_types.proto comments)
    # Map both string names and enum values
    binary_types = {
        # String names
        "MEASUREMENT_TYPE_OK_ALARM",
        "MEASUREMENT_TYPE_FLOODING", 
        "MEASUREMENT_TYPE_OUTPUT_CONTROL",
        # Enum values (from proto_measurement_types.proto)
        5,  # MEASUREMENT_TYPE_OK_ALARM
        7,  # MEASUREMENT_TYPE_FLOODING
        42, # MEASUREMENT_TYPE_OUTPUT_CONTROL
    }
    
    if measurement_type in binary_types:
        return "binary"
    else:
        return "continuous"


def _determine_overall_sensor_type(measurement_types: List) -> str:
    """
    Determine overall sensor type from a list of measurement types.
    
    Args:
        measurement_types: List of measurement types (strings or enum values)
        
    Returns:
        str: "continuous", "binary", or "mixed"
    """
    has_binary = False
    has_continuous = False
    
    for measurement_type in measurement_types:
        if measurement_type:  # Skip empty/None values
            sensor_type = _get_sensor_type_from_measurement_type(measurement_type)
            if sensor_type == "binary":
                has_binary = True
            else:
                has_continuous = True
    
    if has_binary and has_continuous:
        return "mixed"
    elif has_binary:
        return "binary"
    else:
        return "continuous"


def _determine_sensor_type_from_channels(channels: List[Dict[str, Any]]) -> str:
    """
    Determine overall sensor type from channels data.
    
    Args:
        channels: List of channel dictionaries from measurements
        
    Returns:
        str: "continuous", "binary", or "mixed"
    """
    measurement_types = []
    
    for channel in channels:
        if isinstance(channel, dict):
            measure_type = channel.get("type", "")
            if measure_type:
                measurement_types.append(measure_type)
    
    return _determine_overall_sensor_type(measurement_types)


def _calculate_measurement_period(base: int, factor: int, sensor_type: str = "continuous") -> int:
    """
    Calculate measurement period from base and factor parameters according to Efento protobuf specification.
    
    From proto_config.proto:
    - Continuous sensors: measurement_period = base * factor
    - Binary sensors: measurement_period = base (factor is ignored)
    - Backward compatibility: if factor = 0, use default (14 for binary/mixed, 1 for continuous)
    
    Args:
        base: Base measurement period value (Range [1:65535])
        factor: Factor to multiply with base (Range [1:65535], 0 for backward compatibility)
        sensor_type: Type of sensor ("continuous", "binary", or "mixed")
        
    Returns:
        int: Measurement period in seconds
    """
    
    # Validate base
    if base < 1 or base > 65535:
        raise ValueError(f"Base must be in range [1:65535], got {base}")
    
    # Handle backward compatibility for factor = 0
    if factor == 0:
        if sensor_type.lower() in ["binary", "mixed"]:
            factor = 14  # Default for binary/mixed sensors
            logger.debug(f"Using backward compatibility factor 14 for {sensor_type} sensor")
        else:  # continuous
            factor = 1   # Default for continuous sensors
            logger.debug(f"Using backward compatibility factor 1 for {sensor_type} sensor")
    
    # Validate factor after backward compatibility handling
    if factor < 1 or factor > 65535:
        raise ValueError(f"Factor must be in range [1:65535], got {factor}")
    
    # Calculate measurement period based on sensor type
    if sensor_type.lower() == "binary":
        # Binary sensors: measurement_period = base (factor is ignored)
        measurement_period = base
        logger.debug(f"Calculated measurement period for binary sensor: {base}s (factor {factor} ignored)")
    else:
        # Continuous and mixed sensors: measurement_period = base * factor
        measurement_period = base * factor
        logger.debug(f"Calculated measurement period for {sensor_type} sensor: {base} * {factor} = {measurement_period}s")
    
    return measurement_period


def _calculate_period_base_and_factor(period: int) -> tuple[int, int]:
    """
    Calculate optimal base and factor for measurement period.
    
    Args:
        period: Measurement period in seconds
        
    Returns:
        tuple: (base, factor) values for the measurement period
    """
    
    # Validate input
    if period < 1:
        raise ValueError(f"Measurement period must be at least 1 second")
    if period > 65535 * 65535:
        raise ValueError(f"Measurement period too large: {period}s")
    
    # Calculate optimal base and factor
    base, factor = calculate_optimal_base_factor(period)
    
    # Warn if approximation was needed
    actual_period = base * factor
    if actual_period != period:
        logger.warning(f"Requested {period}s approximated as {actual_period}s")
    
    return base, factor

def calculate_optimal_base_factor(period_seconds):
    # Try common base values first for exact matches
    preferred_bases = [1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600]
    
    for base in preferred_bases:
        if period_seconds % base == 0:
            factor = period_seconds // base
            if 1 <= factor <= 65535:
                return base, factor
    
    # Find best approximation if no exact match
    best_base = 1
    best_factor = min(period_seconds, 65535)
    best_error = abs(period_seconds - (best_base * best_factor))
    
    for base in range(1, min(period_seconds + 1, 65536)):
        factor = period_seconds // base
        if factor > 65535 or factor < 1:
            continue
            
        actual_period = base * factor
        error = abs(period_seconds - actual_period)
        
        if error < best_error:
            best_base = base
            best_factor = factor
            best_error = error
    
    return best_base, best_factor

def _format_efento_config_command(config_data: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """
    Format device configuration command to Efento ProtoConfig.

    Args:
        config_data: Configuration parameters dict with any of these supported fields:
            # Measurement Configuration
            - measurement_period: measurement interval in seconds
            - transmission_interval: how often to send data (60-604800 seconds)
            
            # BLE Configuration
            - ble_turnoff_time: BLE turnoff time in seconds (60-604800 or 0xFFFFFFFF for always on)
            - ble_tx_power_level: BLE TX power level (1-4)
            - ble_advertising_period: dict with 'mode', 'normal', 'fast' fields
            
            # Network Configuration
            - data_server_ip: IP or URL of data server (max 31 chars)
            - data_server_port: data server port (1-65535)
            - update_server_ip: IP or URL of update server (max 31 chars)
            - update_server_port_udp: update server UDP port (1-65535)
            - update_server_port_coap: update server CoAP port (1-65535)
            - apn: APN string (max 49 chars, 0x7F for automatic)
            - apn_user_name: APN username (max 31 chars, 0x7F for automatic)
            - apn_password: APN password (max 31 chars, 0x7F for automatic)
            - plmn_selection: PLMN selection (100-999999, 0xFFFFFFFF for automatic)
            - modem_bands_mask: modem bands bitmask
            - dns_server_ip: DNS server IP as list of 4 octets
            - dns_ttl_config: DNS TTL configuration (1-864002)
            - cellular_config_params: list of cellular parameters
            - network_search: dict with network search parameters
            - network_troubleshooting: network troubleshooting mode (1-2)
            
            # Server Communication
            - ack_interval: ACK interval in seconds (180-2592000 or 0xFFFFFFFF)
            - cloud_token_config: cloud token configuration (1, 2, or 255)
            - cloud_token: cloud token string
            - cloud_token_coap_option: CoAP option ID for cloud token (1-64999 or 65000)
            - payload_signature_coap_option: CoAP option ID for payload signature (1-64999 or 65000)
            - transfer_limit: NB-IoT transfer limit (1-65535)
            - transfer_limit_timer: NB-IoT transfer limit timer (1-65535)
            
            # Device Control
            - current_time: current time in epoch seconds
            - supervision_period: supervision period (180-604800 or 0xFFFFFFFF to disable)
            - memory_reset_request: boolean to reset measurement memory
            - disable_modem_request: disable modem for N seconds (60-604800)
            - modem_update_request: modem update URL (max 48 chars)
            - output_control_state_request: list of output control state dicts
            - calibration_parameters_request: calibration parameters dict
            
            # Server Request Flags (server-only fields)
            - request_device_info: boolean to request device info
            - request_fw_update: boolean to request firmware update
            - request_configuration: boolean to request configuration
            - accept_without_testing: boolean to accept config without testing
            - request_runtime_errors_clear: boolean to clear runtime errors
            
            # Endpoints
            - data_endpoint: data endpoint string (max 16 chars)
            - configuration_endpoint: configuration endpoint string (max 16 chars)
            - device_info_endpoint: device info endpoint string (max 16 chars)
            - time_endpoint: time endpoint string (max 16 chars)
            
            # Security
            - encryption_key: encryption key as hex string or bytes (max 16 bytes, 0x7F to disable)
            
            # LED Configuration
            - led_config: list of LED configuration values
            
            # Calendar Configuration
            - calendars: list of calendar configuration dicts
            
        message_parser: ProtobufMessageParser instance
        config: Hardware configuration

    Returns:
        bytes: Serialized ProtoConfig
    """
    logger.debug(f"Creating Efento config command: {config_data}")

    # Build ProtoConfig structure
    proto_config_data = {
        # 'hash': int(time.time()),
        # 'hash_timestamp': int(time.time()),
        'request_device_info': True,
        'request_configuration': True,
        'current_time': int(time.time()),
    }

    # Set measurement periods
    if 'measurement_period' in config_data:
        base, factor = _calculate_period_base_and_factor(config_data['measurement_period'])
        proto_config_data['measurement_period_base'] = base
        proto_config_data['measurement_period_factor'] = factor

    # Add all config_data fields to proto_config_data (excluding special fields handled separately)
    special_fields = {
        'measurement_period', 'dns_server_ip', 'cellular_config_params', 'led_config', 
        'encryption_key', 'ble_advertising_period', 'network_search', 
        'output_control_state_request', 'calibration_parameters_request', 'calendars'
    }
    
    for config_key, value in config_data.items():
        if config_key not in special_fields:
            proto_config_data[config_key] = value

    # Handle special fields that need processing
    
    # DNS Server IP (list of 4 octets)
    if 'dns_server_ip' in config_data:
        dns_ip = config_data['dns_server_ip']
        if isinstance(dns_ip, str):
            # Convert IP string to list of octets
            octets = [int(octet) for octet in dns_ip.split('.')]
            proto_config_data['dns_server_ip'] = octets
        elif isinstance(dns_ip, list):
            proto_config_data['dns_server_ip'] = dns_ip

    # Cellular Config Parameters (repeated uint32)
    if 'cellular_config_params' in config_data:
        params = config_data['cellular_config_params']
        if isinstance(params, list):
            proto_config_data['cellular_config_params'] = params

    # LED Configuration (repeated uint32)
    if 'led_config' in config_data:
        led_config = config_data['led_config']
        if isinstance(led_config, list):
            proto_config_data['led_config'] = led_config

    # Encryption Key (bytes field)
    if 'encryption_key' in config_data:
        value = config_data['encryption_key']
        if isinstance(value, str):
            if value.lower() == 'disabled' or value == '\x7f':
                proto_config_data['encryption_key'] = b'\x7f'
            else:
                try:
                    proto_config_data['encryption_key'] = bytes.fromhex(value)
                except ValueError:
                    # If not valid hex, encode as UTF-8
                    proto_config_data['encryption_key'] = value.encode('utf-8')
        elif isinstance(value, bytes):
            proto_config_data['encryption_key'] = value

    # Create the actual protobuf message structure (Efento-specific logic)
    try:
        # Get the ProtoConfig class from message_parser
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()

        # Set all simple fields from the config data
        for field_name, value in proto_config_data.items():
            if hasattr(proto_message, field_name):
                field_descriptor = proto_message.DESCRIPTOR.fields_by_name.get(field_name)
                if field_descriptor:
                    if field_descriptor.label == field_descriptor.LABEL_REPEATED:
                        # Handle repeated fields
                        if isinstance(value, list):
                            getattr(proto_message, field_name).extend(value)
                        else:
                            getattr(proto_message, field_name).append(value)
                    else:
                        # Handle singular fields
                        setattr(proto_message, field_name, value)

        # Handle complex nested message fields
        
        # BLE Advertising Period
        if 'ble_advertising_period' in config_data:
            ble_config = config_data['ble_advertising_period']
            if isinstance(ble_config, dict):
                ble_msg = proto_message.ble_advertising_period
                if 'mode' in ble_config:
                    ble_msg.mode = ble_config['mode']
                if 'normal' in ble_config:
                    ble_msg.normal = ble_config['normal']
                if 'fast' in ble_config:
                    ble_msg.fast = ble_config['fast']

        # Network Search Configuration
        if 'network_search' in config_data:
            network_config = config_data['network_search']
            if isinstance(network_config, dict):
                network_msg = proto_message.network_search
                if 'time_schema_last_registration_ok' in network_config:
                    network_msg.time_schema_last_registration_ok.extend(
                        network_config['time_schema_last_registration_ok']
                    )
                if 'time_schema_last_registration_not_ok' in network_config:
                    network_msg.time_schema_last_registration_not_ok.extend(
                        network_config['time_schema_last_registration_not_ok']
                    )
                if 'disable_period_base' in network_config:
                    network_msg.disable_period_base = network_config['disable_period_base']
                if 'counter_max' in network_config:
                    network_msg.counter_max = network_config['counter_max']

        # Output Control State Request
        if 'output_control_state_request' in config_data:
            output_states = config_data['output_control_state_request']
            if isinstance(output_states, list):
                for state_dict in output_states:
                    if isinstance(state_dict, dict):
                        state_msg = proto_message.output_control_state_request.add()
                        if 'channel_index' in state_dict:
                            state_msg.channel_index = state_dict['channel_index']
                        if 'channel_state' in state_dict:
                            state_msg.channel_state = state_dict['channel_state']

        # Calibration Parameters Request
        if 'calibration_parameters_request' in config_data:
            cal_config = config_data['calibration_parameters_request']
            if isinstance(cal_config, dict):
                cal_msg = proto_message.calibration_parameters_request
                if 'calibration_request' in cal_config:
                    cal_msg.calibration_request = cal_config['calibration_request']
                if 'channel_assignment' in cal_config:
                    cal_msg.channel_assignment = cal_config['channel_assignment']
                if 'parameters' in cal_config:
                    cal_msg.parameters.extend(cal_config['parameters'])

        # Calendar Configuration (this would need ProtoCalendar message handling)
        if 'calendars' in config_data:
            calendars = config_data['calendars']
            if isinstance(calendars, list):
                logger.warning("Calendar configuration requires ProtoCalendar message handling - not implemented yet")

        # Serialize to bytes
        binary_config = proto_message.SerializeToString()

        if not binary_config:
            raise RuntimeError("Failed to serialize ProtoConfig")

        logger.info(f"Successfully encoded Efento config command: {len(binary_config)} bytes")
        return binary_config

    except Exception as e:
        logger.error(f"Failed to encode Efento config command: {e}")
        raise RuntimeError(f"Config encoding failed: {e}") from e


def _format_efento_firmware_command(firmware_data: dict, config: dict, message_parser: Optional[Any] = None) -> bytes:
    """
    Format firmware update command (placeholder for future implementation).

    Args:
        firmware_data: Firmware update parameters
        message_parser: ProtobufMessageParser instance
        config: Hardware configuration

    Returns:
        bytes: Serialized command
    """
    logger.warning("Firmware update commands not yet implemented for Efento devices")
    raise NotImplementedError("Efento firmware updates not implemented")


# --- RESPONSE FORMATTING FUNCTION ---

def format_response(config: dict, message_parser: Optional[Any] = None) -> bytes:
    """
    Generate Efento-specific response payload for CoAP communication.
    
    This function handles both command responses and acknowledgment responses:
    - When command is present: generates command-specific response
    - When command is None: generates acknowledgment response
    
    For Efento devices, all responses include current_time synchronization.
    
    Args:
        command: Optional command dictionary that was sent to device:
            - command_type: Type of command (e.g., "alarm", "config")
            - device_id: Target device identifier
            - payload: Command-specific data
            - metadata: Additional command context
        message_parser: ProtobufMessageParser instance for Efento communication
        config: Hardware configuration dictionary
        
    Returns:
        bytes: Protobuf-encoded response payload for CoAP transmission
        
    Raises:
        RuntimeError: If response generation fails
    """    
    try:
        # Get the ProtoConfig class from message_parser
        proto_class = message_parser.proto_modules["config"]["class"]
        proto_message = proto_class()
        
        # Set current time (Efento-specific requirement for all responses)
        current_time = int(time.time())
        proto_message.current_time = current_time
             
        # Standard acknowledgment response (no command present)
        logger.debug("Generating acknowledgment response")
        proto_message.request_device_info = True
        proto_message.request_configuration = True
        proto_message.ack_interval = 0xFFFFFFFF
            
        # Serialize to bytes
        response_bytes = proto_message.SerializeToString()
        
        if not response_bytes:
            raise RuntimeError("Failed to serialize Efento response")
            
        logger.info(f"Generated Efento response: {len(response_bytes)} bytes, current_time: {current_time}")
        logger.debug(f"Response hex: {response_bytes.hex()}")
        
        return response_bytes
        
    except Exception as e:
        logger.error(f"Failed to generate Efento response: {e}")
        raise RuntimeError(f"Efento response generation failed: {e}") from e
