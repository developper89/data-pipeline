# storage/parser_scripts/efento_bidirectional_parser.py

import base64
import datetime
import math
import logging
import time
from typing import List, Dict, Any, Optional

# Required imports from libraries that must be available in the execution environment
from google.protobuf.message import DecodeError
from google.protobuf.json_format import MessageToDict

# Import your compiled protobuf definitions
# These must be importable in the script's execution environment
try:
    # Try container-style direct import first
    import sys
    import os
    
    # Add container protobuf path if available
    container_protobuf_path = "/app/shared/translation/protobuf/proto_schemas/efento/protobuf"
    if os.path.exists(container_protobuf_path) and container_protobuf_path not in sys.path:
        sys.path.insert(0, container_protobuf_path)
    
    # Direct import (container style)
    import proto_measurements_pb2
    import proto_device_info_pb2
    import proto_config_pb2
    import proto_rule_pb2
    PROTOBUF_AVAILABLE = True
    print(f"✅ Parser: Protobuf modules imported successfully from {container_protobuf_path}")
    
except ImportError:
    try:
        # Fallback: try original package-style import
        from protobuf import proto_measurements_pb2
        from protobuf import proto_device_info_pb2
        from protobuf import proto_config_pb2
        from protobuf import proto_rule_pb2
        PROTOBUF_AVAILABLE = True
        print("✅ Parser: Protobuf modules imported via fallback package import")
    except ImportError as e:
        PROTOBUF_AVAILABLE = False
        print(f"❌ Parser: Protobuf import failed: {e}")
        # Define dummy classes if protos aren't available, so script loads but fails gracefully
        class DummyProto:
            def ParseFromString(self, data): pass
        proto_measurements_pb2 = type("proto_measurements_pb2", (object,), {"ProtoMeasurements": DummyProto})
        proto_device_info_pb2 = type("proto_device_info_pb2", (object,), {"ProtoDeviceInfo": DummyProto})
        proto_config_pb2 = type("proto_config_pb2", (object,), {"ProtoConfig": DummyProto})
        proto_rule_pb2 = type("proto_rule_pb2", (object,), {"CONDITION_HIGH_THRESHOLD": 2, "ACTION_TRIGGER_TRANSMISSION": 1})


# Configure optional logging within the script
script_logger = logging.getLogger("efento_parser_script")
# Example: Basic config if running standalone for tests
# logging.basicConfig()
# script_logger.setLevel(logging.DEBUG)


# --- Helper Functions (Adapted from CoapNormalizer) ---

def _normalize_measurements(decoded_data: Dict[str, Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalization logic for decoded measurements data.
    Creates a list of dictionaries matching StandardizedOutput structure.
    """
    output_list = []
    base_device_id = config.get('device_id', 'UNKNOWN_DEVICE_ID') # Get from config
    base_labels = config.get('labels') # Get from config

    # Extract top-level info from decoded protobuf data
    # Use .get() with defaults for safety
    serial_num_b64 = decoded_data.get("serialNum", "") # Protobuf uses 'serialNum'
    serial_num_hex = base64.b64decode(serial_num_b64).hex() if serial_num_b64 else base_device_id # Fallback
    # If serial_num_hex is different from device_id from config, it might indicate an issue
    if serial_num_hex != base_device_id:
        script_logger.warning(f"Device ID mismatch: Config '{base_device_id}', Payload SN '{serial_num_hex}'")

    battery_status = decoded_data.get("batteryStatus", False) # Protobuf uses 'batteryStatus'
    measurement_period_base = decoded_data.get("measurementPeriodBase", 1)
    measurement_period_factor = decoded_data.get("measurementPeriodFactor", 1)
    measurement_period = measurement_period_base * measurement_period_factor

    channels = decoded_data.get("channels", [])
    major_values = []
    calibration_required = []
    current_channel_idx_for_acc = 0 # Track separate index for ACC types if needed

    for channel_idx, channel_data in enumerate(channels, start=1):
        if not channel_data or not isinstance(channel_data, dict):
            continue

        measure_type = channel_data.get("type", "")
        timestamp_base = channel_data.get("timestamp", 0)
        start_point = channel_data.get("startPoint", 0)
        sample_offsets = channel_data.get("sampleOffsets", [])

        if not measure_type or not sample_offsets:
             script_logger.warning(f"Skipping channel {channel_idx}: Missing type or sampleOffsets")
             continue

        # --- Pre-process Major Counters ---
        # We need to iterate once to build these if minor counters depend on them
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
                else: # Assuming Water Meter
                    val = math.floor((start_point + sample_offset) / 4) * 100
                temp_major_values.append(val)
                temp_calibration_required.append(meta_data != 0)
            # Store these for potential use by subsequent minor channels
            major_values = temp_major_values
            calibration_required = temp_calibration_required
            current_channel_idx_for_acc = channel_idx # Associate these majors with this channel index
            script_logger.debug(f"Processed Major values for channel {channel_idx}")
            # Optionally, decide if MAJOR values themselves should be reported
            # For now, assume they are only used for MINOR calculations

        # --- Process Minor Accumulators (using previous Major) ---
        elif measure_type in (
            "MEASUREMENT_TYPE_PULSE_CNT_ACC_MINOR",
            "MEASUREMENT_TYPE_WATER_METER_ACC_MINOR",
            "MEASUREMENT_TYPE_ELEC_METER_ACC_MINOR",
        ):
            # Check if corresponding major values are available
            if not major_values or channel_idx <= current_channel_idx_for_acc: # Ensure major came before minor
                 script_logger.warning(f"Minor ACC channel {channel_idx} found without preceding Major values. Skipping.")
                 continue

            output_index = measure_type.replace("MEASUREMENT_TYPE_", "").replace("_MINOR", "")
            for idx, sample_offset in enumerate(sample_offsets):
                 if idx >= len(major_values):
                      script_logger.warning(f"Index {idx} out of bounds for major values ({len(major_values)}) on channel {channel_idx}. Skipping offset.")
                      continue

                 # Summation logic with calibration check
                 if calibration_required[idx]:
                      final_value = major_values[idx] + math.floor((start_point + sample_offset) / 6)
                      value_metadata = {"calibration_required": True}
                 else:
                      final_value = major_values[idx] + ((start_point + sample_offset) // 6)
                      value_metadata = {"calibration_required": False}

                 # Careful: Timestamp uses measurement_period * idx
                 reading_dt = datetime.datetime.fromtimestamp(timestamp_base + (measurement_period * idx), tz=datetime.timezone.utc)

                 output_dict = {
                      "device_id": base_device_id,
                      "values": [final_value], # Value goes into the list
                      "label": base_labels, # Optional labels from config
                      "index": output_index, # Type of measurement
                      "timestamp": reading_dt, # Datetime object
                      "metadata": { # Additional context
                           "battery_ok": battery_status,
                           "channel_number": channel_idx,
                           "source_sn_hex": serial_num_hex, # Include SN from payload
                           **value_metadata # Add calibration info
                      }
                 }
                 output_list.append(output_dict)
            # Clear major values after use? Depends if one major applies to multiple minors.
            # Assuming one major per minor for now.
            major_values = []
            calibration_required = []

        # --- Process OK/Alarm ---
        elif measure_type == "MEASUREMENT_TYPE_OK_ALARM":
            # Logic to determine state at different times based on change offsets
            change_points = {} # time -> state ("OK" or "Alarm")
            for sample_offset in sample_offsets:
                time_at_change = timestamp_base + abs(sample_offset) - 1
                state = "Alarm" if sample_offset > 0 else "OK"
                change_points[time_at_change] = state

            # Create readings for each interval or specific change points
            # Simplification: Report only the change points found
            sorted_times = sorted(change_points.keys())
            for t in sorted_times:
                state = change_points[t]
                reading_dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
                output_dict = {
                     "device_id": base_device_id,
                     "values": [state],
                     "label": base_labels,
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
            output_index = measure_type.replace("MEASUREMENT_TYPE_", "")
            for idx, sample_offset in enumerate(sample_offsets):
                # Check for sensor error codes (based on real sensor data analysis)
                # Error range: 8355840 to 8388607 (sensor failure conditions)
                error_range_start = 8355840
                error_range_end = 8388607
                is_error = error_range_start <= sample_offset <= error_range_end
                
                if is_error:
                    # Handle sensor error condition
                    final_value = None  # No valid measurement
                    value_metadata = {
                        "is_error": True,
                        "error_code": sample_offset,
                        "error_description": f"Sensor failure (code {sample_offset})",
                        "battery_ok": battery_status,
                        "channel_number": channel_idx,
                        "source_sn_hex": serial_num_hex,
                    }
                    script_logger.warning(f"Sensor error detected: Channel {channel_idx}, Code {sample_offset}")
                else:
                    # Normal measurement processing
                    if measure_type in ("MEASUREMENT_TYPE_TEMPERATURE", "MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE"):
                         final_value = (start_point + sample_offset) / 10.0
                    else:
                         final_value = start_point + sample_offset # Assuming other types are direct values
                    
                    value_metadata = {
                        "is_error": False,
                        "battery_ok": battery_status,
                        "channel_number": channel_idx,
                        "source_sn_hex": serial_num_hex,
                    }

                reading_dt = datetime.datetime.fromtimestamp(timestamp_base + (measurement_period * idx), tz=datetime.timezone.utc)

                output_dict = {
                     "device_id": base_device_id,
                     "values": [final_value] if final_value is not None else [], # Value goes into the list, empty for errors
                     "label": base_labels,
                     "index": output_index, # Type of measurement
                     "timestamp": reading_dt, # Datetime object
                     "metadata": value_metadata # Include error detection metadata
                }
                output_list.append(output_dict)

    return output_list


def _normalize_device_info(decoded_data: Dict[str, Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalizes device info into a single StandardizedOutput dictionary.
    The full decoded info is placed in the metadata field.
    """
    base_device_id = config.get('device_id', 'UNKNOWN_DEVICE_ID')
    base_labels = config.get('labels')

    serial_num_b64 = decoded_data.get("serialNum", "")
    serial_num_hex = base64.b64decode(serial_num_b64).hex() if serial_num_b64 else base_device_id

    output_dict = {
        "device_id": base_device_id,
        "values": ["device_info_received"], # Simple marker value
        "label": base_labels,
        "index": "DEVICE_INFO", # Specific index for this type
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc), # Use processing time
        "metadata": {
            "source_sn_hex": serial_num_hex,
            "full_device_info": decoded_data # Embed the full decoded structure
        }
    }
    return [output_dict] # Return as a list with one item


def _normalize_config(decoded_data: Dict[str, Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalizes device config confirmation into a single StandardizedOutput dictionary.
    The full decoded config is placed in the metadata field.
    """
    base_device_id = config.get('device_id', 'UNKNOWN_DEVICE_ID')
    base_labels = config.get('labels')

    output_dict = {
        "device_id": base_device_id, # Associate with the intended device
        "values": ["config_info_received"], # Simple marker value
        "label": base_labels,
        "index": "DEVICE_CONFIG_CONFIRM", # Specific index for this type
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc), # Use processing time
        "metadata": {
             "full_config_info": decoded_data # Embed the full decoded structure
        }
    }
    return [output_dict] # Return as a list with one item


# --- MANDATORY PARSE FUNCTION ---

def parse(payload: bytes, config: dict) -> List[dict]:
    """
    Parses raw payload based on CoAP path hint in config metadata,
    decodes Efento protobuf message, and normalizes it into a list of
    dictionaries matching the StandardizedOutput structure.

    Args:
        payload: bytes of the raw payload.
        config: The device configuration dictionary, expected to contain
                'device_id', optionally 'labels', and potentially
                'metadata' with 'uri_path' hinting the payload type.

    Returns:
        A list of dictionaries formatted for StandardizedOutput validation.

    Raises:
        ValueError: If input types are wrong, decoding fails, path is missing/unknown,
                    or normalization fails.
        KeyError: If required 'device_id' is missing from config.
        ImportError: If protobuf libraries/definitions are not found.
        Exception: For other unexpected errors.
    """
    if not PROTOBUF_AVAILABLE:
        raise ImportError("Protobuf libraries or definitions not found. Cannot parse.")

    script_logger.debug(f"Parser script received payload: {payload[:50]}...") # Log snippet
    script_logger.debug(f"Parser script received config: {config}")

    # --- 1. Input Validation & Setup ---
    if not isinstance(payload, bytes):
        raise ValueError("payload must be a bytes.")
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary.")

    try:
        device_id = config['device_id'] # Mandatory
    except KeyError:
        raise KeyError("Required key 'device_id' missing from config.")

    try:
        payload_bytes = payload
    except (TypeError, ValueError) as e:
        raise ValueError(f"Failed to decode base64 payload: {e}") from e

    # --- 2. Determine Payload Type (from CoAP path hint in metadata) ---
    metadata = config.get('metadata', {})
    uri_path = metadata.get('uri_path') # e.g., '/m', '/i', '/c', '/data/device_id'

    if not uri_path:
        # Fallback or error if path is missing - this is crucial
        # Option: Could try decoding as measurement by default? Risky.
        raise ValueError("Missing 'uri_path' in config metadata to determine payload type.")

    # Simplify path for matching (e.g., get primary resource indicator)
    path_segment = uri_path.strip('/').split('/')[0] # Get first segment ('m', 'i', 'c', 'data', etc.)

    # --- 3. Decode Payload based on Type ---
    decoded_data: Optional[Dict[str, Any]] = None
    try:
        if path_segment == 'm': # Assuming '/m' for measurements
            script_logger.debug("Decoding as ProtoMeasurements")
            proto_obj = proto_measurements_pb2.ProtoMeasurements()
            proto_obj.ParseFromString(payload_bytes)
            decoded_data = MessageToDict(proto_obj, preserving_proto_field_name=True)
            decoder_type = "measurements"
        elif path_segment == 'i': # Assuming '/i' for device info
            script_logger.debug("Decoding as ProtoDeviceInfo")
            proto_obj = proto_device_info_pb2.ProtoDeviceInfo()
            proto_obj.ParseFromString(payload_bytes)
            decoded_data = MessageToDict(proto_obj, preserving_proto_field_name=True)
            decoder_type = "device_info"
        elif path_segment == 'c': # Assuming '/c' for config
            script_logger.debug("Decoding as ProtoConfig")
            proto_obj = proto_config_pb2.ProtoConfig()
            proto_obj.ParseFromString(payload_bytes)
            decoded_data = MessageToDict(proto_obj, preserving_proto_field_name=True)
            decoder_type = "config"
        # Add elif for '/t' or other paths if they carry parsable payloads
        elif path_segment == 't': # Time synchronization
            script_logger.debug("Time request, no parsing needed")
            decoder_type = "time"
            decoded_data = {"timestamp": int(time.time())}
        else:
            raise ValueError(f"Unknown or unsupported CoAP path segment for parsing: '{path_segment}'")

    except DecodeError as e:
        raise ValueError(f"Protobuf DecodeError for path '{path_segment}': {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error during decoding for path '{path_segment}': {e}") from e

    if decoded_data is None:
        raise ValueError(f"Decoding failed, resulted in None for path '{path_segment}'")

    # --- 4. Normalize Decoded Data based on Type ---
    normalized_output_list: List[Dict[str, Any]] = []
    try:
        if decoder_type == "measurements":
            normalized_output_list = _normalize_measurements(decoded_data, config)
        elif decoder_type == "device_info":
             normalized_output_list = _normalize_device_info(decoded_data, config)
        elif decoder_type == "config":
             normalized_output_list = _normalize_config(decoded_data, config)
        elif decoder_type == "time":
            # No normalization needed for time requests
            normalized_output_list = []
        # Add other normalization cases if needed
        else:
             # Should not happen if decoding worked, but safety check
             raise RuntimeError(f"Decoder type '{decoder_type}' not handled in normalization.")

    except Exception as e:
         # Catch errors during the normalization process
         script_logger.exception(f"Error during normalization of {decoder_type} data: {e}")
         raise RuntimeError(f"Normalization failed for {decoder_type}: {e}") from e

    script_logger.info(f"Successfully parsed and normalized {len(normalized_output_list)} records.")
    script_logger.debug(f"Returning: {normalized_output_list}")
    return normalized_output_list


# --- NEW FUNCTION FOR COMMAND FORMATTING ---

def format_command(command: dict, config: dict) -> bytes:
    """
    Formats a command from standardized format to device-specific binary format.
    
    Args:
        command: The command to format, containing fields like command_type, payload, etc.
        config: The device configuration dictionary, similar to parse function.
        
    Returns:
        bytes: The formatted command as binary data ready to be sent to the device.
        
    Raises:
        ValueError: If the command type is unknown or required fields are missing.
        ImportError: If protobuf libraries are not available.
        Exception: For other unexpected errors.
    """
    if not PROTOBUF_AVAILABLE:
        raise ImportError("Protobuf libraries or definitions not found. Cannot format command.")
        
    script_logger.debug(f"Formatting command: {command}")
    
    # Extract command details
    command_type = command.get('command_type')
    payload = command.get('payload', {})
    
    if not command_type:
        raise ValueError("Command must have a command_type field")
        
    # Format based on command type
    if command_type == 'config_update':
        # Create a configuration protobuf message
        proto_config = proto_config_pb2.ProtoConfig()
        
        # Always set current time
        proto_config.current_time = int(time.time())
        
        # Set basic configuration from payload
        proto_config.request_device_info = payload.get('request_device_info', True)
        
        # Set measurement period (in seconds, not minutes)
        if 'measurement_interval' in payload:
            # Set measurement period base and factor
            # For simplicity, use factor of 1 and set base to the full interval
            proto_config.measurement_period_base = payload['measurement_interval']
            proto_config.measurement_period_factor = 1
            
        # Set transmission interval (in seconds)
        if 'transmission_interval' in payload:
            proto_config.transmission_interval = payload['transmission_interval']
            
        # Set ACK interval
        if 'ack_interval' in payload:
            ack_interval = payload['ack_interval']
            if ack_interval == 'always':
                proto_config.ack_interval = 0xFFFFFFFF  # Always request ACK
            else:
                try:
                    proto_config.ack_interval = int(ack_interval)
                except ValueError:
                    proto_config.ack_interval = 86400  # Default to 24 hours in seconds
        
        # Set server configuration if provided
        if 'server_address' in payload:
            proto_config.data_server_ip = payload['server_address']
        
        if 'server_port' in payload:
            proto_config.data_server_port = payload['server_port']
        
        # Set APN configuration
        if 'apn' in payload and payload['apn'] != 'auto':
            proto_config.apn = payload['apn']
        
        # Set PLMN configuration
        if 'plmn' in payload and payload['plmn'] != 'auto':
            proto_config.plmn_selection = payload['plmn']
        
        # Set Bluetooth configuration
        if 'bluetooth_turn_off_time' in payload and payload['bluetooth_turn_off_time'] > 0:
            proto_config.ble_turnoff_time = payload['bluetooth_turn_off_time']
        
        # Set transfer limit
        if payload.get('transfer_limit_enabled', False):
            proto_config.transfer_limit = payload.get('transfer_limit_max', 5)
            proto_config.transfer_limit_timer = payload.get('transfer_limit_period', 3600)  # In seconds
        
        # Add rules if any
        if 'rules' in payload and isinstance(payload['rules'], list):
            for rule_data in payload['rules']:
                rule = proto_config.rules.add()
                
                # Set channel mask (which channels this rule applies to)
                channel = rule_data.get('channel', 1)
                rule.channel_mask = 1 << (channel - 1)  # Convert channel number to bit mask
                
                # Set rule condition
                rule_type = rule_data.get('type')
                if rule_type == 'above_threshold':
                    rule.condition = proto_rule_pb2.CONDITION_HIGH_THRESHOLD
                elif rule_type == 'below_threshold':
                    rule.condition = proto_rule_pb2.CONDITION_LOW_THRESHOLD
                elif rule_type == 'differential_threshold':
                    rule.condition = proto_rule_pb2.CONDITION_DIFF_THRESHOLD
                elif rule_type == 'on_measurement':
                    rule.condition = proto_rule_pb2.CONDITION_ON_MEASUREMENT
                else:
                    rule.condition = proto_rule_pb2.CONDITION_DISABLED
                
                # Set rule parameters based on condition type
                if rule_type in ['above_threshold', 'below_threshold']:
                    # parameter[0] - Threshold value
                    # parameter[1] - Hysteresis value
                    # parameter[2] - Triggering mode (1=moving avg, 2=window avg, 3=consecutive)
                    # parameter[3] - Number of measurements
                    # parameter[4] - Measurement type
                    rule.parameters.extend([
                        int(rule_data.get('threshold', 0) * 10),  # Convert to device encoding
                        int(rule_data.get('hysteresis', 0) * 10),  # Convert to device encoding
                        rule_data.get('triggering_mode', 1),  # Default to moving average
                        rule_data.get('consecutive_measurements', 1),
                        1  # Temperature type (example)
                    ])
                elif rule_type == 'differential_threshold':
                    # parameter[0] - Threshold value
                    # parameter[1] - Triggering mode
                    # parameter[2] - Number of measurements
                    # parameter[3] - Measurement type
                    rule.parameters.extend([
                        int(rule_data.get('threshold', 0) * 10),  # Convert to device encoding
                        rule_data.get('triggering_mode', 1),
                        rule_data.get('consecutive_measurements', 1),
                        1  # Temperature type (example)
                    ])
                elif rule_type == 'on_measurement':
                    # parameter[0] - Send every n measurement
                    rule.parameters.extend([
                        rule_data.get('measurement_interval', 1)
                    ])
                
                # Set rule action
                action = rule_data.get('action')
                if action == 'trigger_transmission':
                    rule.action = proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION
                elif action == 'trigger_transmission_with_ack':
                    rule.action = proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION_WITH_ACK
                else:
                    rule.action = proto_rule_pb2.ACTION_NO_ACTION
        
        # Return serialized protobuf message
        return proto_config.SerializeToString()
    
    elif command_type == 'time_sync':
        # Format response for time request
        time_stamp = int(time.time())
        time_stamp_hex = hex(time_stamp)[2:]  # Remove '0x' prefix
        
        # Return as bytes
        return bytes.fromhex(time_stamp_hex)
    
    else:
        raise ValueError(f"Unknown command type: {command_type}")


# --- Local Testing Block ---
if __name__ == '__main__':
    print("--- Running Efento Bidirectional Parser Tests ---")
    
    if not PROTOBUF_AVAILABLE:
        print("WARNING: Protobuf libraries not available. Tests will be limited.")
    
    # Test the format_command function
    print("\n--- Testing Command Formatting ---")
    
    # Test case 1: Config Update Command
    test_config_command = {
        'command_type': 'config_update',
        'payload': {
            'measurement_interval': 900,  # 15 minutes in seconds
            'transmission_interval': 3600,  # 1 hour in seconds
            'ack_interval': 'always', 
            'server_address': '192.168.1.100',
            'server_port': 5683,
            'rules': [
                {
                    'type': 'above_threshold',
                    'channel': 1,
                    'threshold': 25.0,
                    'hysteresis': 2.0,
                    'action': 'trigger_transmission'
                }
            ]
        }
    }
    
    test_device_config = {
        'device_id': 'aabbccddeeff',
        'labels': {'room': 'server-room'}
    }
    
    try:
        if PROTOBUF_AVAILABLE:
            # Format the command
            formatted_command = format_command(test_config_command, test_device_config)
            print(f"SUCCESS: Command formatted, binary length: {len(formatted_command)} bytes")
            
            # Verify by decoding back
            proto_obj = proto_config_pb2.ProtoConfig()
            proto_obj.ParseFromString(formatted_command)
            decoded = MessageToDict(proto_obj, preserving_proto_field_name=True)
            print("Verification - decoded command data:")
            for key, value in decoded.items():
                print(f"  {key}: {value}")
                
            # Check key fields
            assert proto_obj.measurement_period_base == 900  # 900 seconds
            assert proto_obj.measurement_period_factor == 1  # Factor of 1
            assert proto_obj.transmission_interval == 3600  # 3600 seconds
            assert proto_obj.ack_interval == 0xFFFFFFFF  # Always request ACK
            print("Verification passed!")
        else:
            print("Skipping test due to missing protobuf libraries")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    # Test case 2: Time Sync Command
    test_time_command = {
        'command_type': 'time_sync',
        'payload': {}
    }
    
    try:
        # Format the time command
        formatted_time = format_command(test_time_command, test_device_config)
        print(f"\nSUCCESS: Time command formatted, binary length: {len(formatted_time)} bytes")
        
        # Verify by converting back to timestamp
        hex_time = formatted_time.hex()
        timestamp = int(hex_time, 16)
        current_time = int(time.time())
        time_diff = abs(current_time - timestamp)
        
        print(f"Verification - decoded timestamp: {timestamp} (current: {current_time}, diff: {time_diff}s)")
        assert time_diff < 5  # Should be within 5 seconds
        print("Time command verification passed!")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc() 