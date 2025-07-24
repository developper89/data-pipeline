import binascii
import json
import datetime
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger("parser_script")
# ---------------------------------------------------------
# Configuration & JSON Encoder
# ---------------------------------------------------------

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)

# ---------------------------------------------------------
# Decoder - responsible for low-level parsing
# ---------------------------------------------------------
class Decoder:
    @staticmethod
    def _rev_bytes_to_int(b: bytes) -> int:
        """Reverse-order byte parsing to int."""
        return int.from_bytes(b, byteorder="little")

    @staticmethod
    def _extract_channel_data(sen_typ: int, raw_data: List[int], config: dict) -> List[Dict[str, Any]]:
        """
        Extracts channel-related info from the sensor type bits (sen_typ)
        and raw channel data (raw_data).
        """
        channels = []
        for ch_index in range(4):
            channel_bits = (sen_typ >> (ch_index * 8)) & 0xFF
            ch_type = channel_bits & 0x1F
            reg_t = bool((channel_bits >> 5) & 1)
            reg_u = bool((channel_bits >> 6) & 1)
            ext_tu = bool((channel_bits >> 7) & 1)

            data_raw = raw_data[ch_index]
            decoded = {}
            # If recognized sensor type in config, decode as needed
            sensor_type = config["sen_typ"].get(str(ch_type), "")
            low = data_raw & 0xFFFF
            high = (data_raw >> 16) & 0xFFFF
            
            if sensor_type:  # recognized
                # Define parsing functions for each sensor type
                def parse_temperature(val):
                    return (val - 10000) / 100
                
                def parse_humidity(val):
                    return val / 100
                
                # Mapping of sensor types to their parsing functions
                parsers = {
                    'T': parse_temperature,
                    'U': parse_humidity
                }
                
                # For 2-letter sensor (e.g. "TU"), decode as 2 separate values
                if len(sensor_type) > 1:
                    parsed_values = []
                    for index, sensor_char in enumerate(sensor_type):
                        if sensor_char in parsers:
                            value = low if index == 0 else high
                            parsed_values.append(parsers[sensor_char](value))
                        else:
                            # Fallback for unknown sensor characters
                            parsed_values.append(low if index == 0 else high)
                    decoded["values"] = parsed_values
                else:
                    # Single sensor type - combine low and high values
                    if sensor_type in parsers:
                        combined_value = low + high
                        decoded["values"] = [parsers[sensor_type](combined_value)]
                    else:
                        # Fallback for unknown sensor types
                        decoded["values"] = [low + high]
            else:  # unknown sensor type
                decoded["values"] = [data_raw]

            # Only append channel info if type > 0
            if ch_type > 0:
                channels.append({
                    "channel_num": ch_index + 1,
                    "type": ch_type,
                    "reg_T": reg_t,
                    "reg_U": reg_u,
                    "external_TU": ext_tu,
                    **decoded
                })

        return channels

    @staticmethod
    def decode(data: bytes, config: dict) -> Dict[str, Any]:
        """
        Decodes the given hex payload into a Python dictionary with
        meaningful fields.
        """
        # data = binascii.unhexlify(payload_hex)

        # Extract fields from data
        d = {
            "id":              Decoder._rev_bytes_to_int(data[0:4]),
            "hw_type":         Decoder._rev_bytes_to_int(data[4:5]),
            "reg_typ":         Decoder._rev_bytes_to_int(data[5:7]),
            "sen_typ":         Decoder._rev_bytes_to_int(data[7:11]),
            "U_consigne":      Decoder._rev_bytes_to_int(data[11:13]) / 100,
            "T_consigne":      Decoder._rev_bytes_to_int(data[14:16]) / 100,
            "U_hysteresys":    (Decoder._rev_bytes_to_int(data[13:14]) - 127) / 10,
            "T_hysteresys":    (Decoder._rev_bytes_to_int(data[16:17]) - 127) / 10,
            "T_int_offset":    Decoder._rev_bytes_to_int(data[17:18]),
            "U_int_offset":    Decoder._rev_bytes_to_int(data[18:19]),
            "T_ext_offset":    Decoder._rev_bytes_to_int(data[19:20]),
            "U_ext_offset":    Decoder._rev_bytes_to_int(data[20:21]),
        }

        ch_data = [
            Decoder._rev_bytes_to_int(data[21:25]),
            Decoder._rev_bytes_to_int(data[25:29]),
            Decoder._rev_bytes_to_int(data[29:33]),
            Decoder._rev_bytes_to_int(data[33:37])
        ]
        d.update({
            "CH1_DATA": ch_data[0],
            "CH2_DATA": ch_data[1],
            "CH3_DATA": ch_data[2],
            "CH4_DATA": ch_data[3],
        })

        # Index after channels
        idx = 37

        # If hw_type LSB set, parse MEM_VA as 8 bytes next
        if d["hw_type"] & 1:
            d["MEM_VA"] = Decoder._rev_bytes_to_int(data[idx:idx+8])
            idx += 8
            c = Decoder._rev_bytes_to_int(data[idx:idx+2])
            idx += 2
            d["status"] = Decoder._rev_bytes_to_int(data[idx:idx+1])
            idx += 1
        else:
            c = Decoder._rev_bytes_to_int(data[idx:idx+2])
            idx += 2
            d["status"] = Decoder._rev_bytes_to_int(data[idx:idx+4])
            idx += 4

        # Convert control bits
        d["control"] = [bool(c & (1 << i)) for i in range(8)]

        # Create states array
        d["states"] = [
            bool(d["control"][4] and d["control"][5]),
            bool(d["control"][6] and d["control"][7]),
            bool(not d["control"][4] and not d["control"][5]),
            bool(not d["control"][6] and not d["control"][7]),
            bool(not d["control"][4] and d["control"][5]),
            bool(not d["control"][6] and d["control"][7]),
            bool(d["control"][4] and not d["control"][5]),
            bool(d["control"][6] and not d["control"][7])
        ]

        # Extract channel info
        d["channels"] = Decoder._extract_channel_data(d["sen_typ"], ch_data, config)

        return d

# ---------------------------------------------------------
# Dataclass for standardized sensor reading
# ---------------------------------------------------------
@dataclass
class SensorReading:
    """
    Represents sensor readings with a consistent structure:
    device_id, values, timestamp, metadata, index, optional label.
    """
    device_id: str
    values: List[Any]
    labels: Optional[List[str]] = field(default=None)
    display_names: Optional[List[str]] = field(default=None)
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    index: str = field(default="")

# ---------------------------------------------------------
# Normalizer - transforms parsed data into SensorReading(s)
# ---------------------------------------------------------
class Normalizer:
    @staticmethod
    def _settings_reading(parsed_data: Dict[str, Any]) -> List[SensorReading]:
        """
        Creates two separate SensorReading objects:
        1. One for consigne values (index "P")
        """
        device_id = str(parsed_data["id"])
        
        # Check if sensor is active (bit 14 of reg_typ)
        reg_type = parsed_data["reg_typ"]
        reg_type_state = bool(reg_type & (1 << 14))
        
        # Check if sensor has TEC capability (bit 2 of reg_typ)
        has_tec = bool(reg_type & (1 << 2))
        
        # Build params_keys and params_values based on sensor capabilities
        params_keys = []
        params_values = []
        
        # Temperature fields - only if TEC is enabled (bit 2)
        if has_tec:
            params_keys.extend(["T_consigne", "T_hysteresys"])
            params_values.extend([parsed_data["T_consigne"], parsed_data["T_hysteresys"]])
        
        # Humidity fields - always present
        params_keys.extend(["U_consigne", "U_hysteresys"])
        params_values.extend([parsed_data["U_consigne"], parsed_data["U_hysteresys"]])
        
        # Active state - always present
        params_keys.append("reg_type_state")
        params_values.append(reg_type_state)
        
        return SensorReading(
            device_id=device_id, 
            labels=params_keys,
            values=params_values, 
            index="P",
            metadata={
                "hw_type": parsed_data["hw_type"],
                "sen_typ": parsed_data["sen_typ"],
                "reg_type": reg_type,
                "reg_type_state": reg_type_state,
                "has_tec": has_tec
            }
        )

    @staticmethod
    def _control_reading(parsed_data: Dict[str, Any]) -> SensorReading:
        device_id = str(parsed_data["id"])
        values = parsed_data["control"]
        metadata = {}

        # Optionally store MEM_VA
        if "MEM_VA" in parsed_data:
            metadata["MEM_VA"] = parsed_data["MEM_VA"]

        # Control bit labels in the same order as the values
        control_labels = [
            "BIT_REG_U_D",
            "BIT_REG_U_H",
            "BIT_REG_TEC_COOL",
            "BIT_REG_TEC_HEAT",
            "BIT_REG_LIGHT_CTRL1",
            "BIT_REG_LIGHT_PRES1",
            "BIT_REG_LIGHT_CTRL2",
            "BIT_REG_LIGHT_PRES2"
        ]

        return SensorReading(device_id, values, labels=control_labels, metadata=metadata, index="CTRL")

    @staticmethod
    def _status_reading(parsed_data: Dict[str, Any]) -> SensorReading:
        device_id = str(parsed_data["id"])
        status_value = parsed_data["status"]
        # Convert status to bits if it's an int
        if isinstance(status_value, int):
            status_bits = []
            # Example: up to 32 bits if needed
            for i in range(32):
                status_bits.append(bool((status_value >> i) & 1))
            values = status_bits
            
            # Status bit labels in the same order as the bits are extracted (bit 0 to bit 31)
            status_labels = [
                "BIT_ERROR_I2C_CH1_NO_SENSOR",
                "BIT_ERROR_I2C_CH1_MES",
                "BIT_ERROR_I2C_CH2_NO_SENSOR",
                "BIT_ERROR_I2C_CH2_MES",
                "BIT_ERROR_I2C_CH3_NO_SENSOR",
                "BIT_ERROR_I2C_CH3_MES",
                "BIT_ERROR_I2C_CH4_NO_SENSOR",
                "BIT_ERROR_I2C_CH4_MES",
                "BIT_ERROR_CH1_CTRL",
                "BIT_ERROR_CH1_FAN",
                "BIT_ERROR_CH2_CTRL",
                "BIT_ERROR_CH2_FAN",
                "BIT_ERROR_CH3_CTRL",
                "BIT_ERROR_CH3_FAN",
                "BIT_ERROR_CH4_CTRL",
                "BIT_ERROR_CH4_FAN",
                "BIT_ERROR_BASE_BIT_0",
                "BIT_ERROR_BASE_BIT_1",
                "BIT_ERROR_BASE_BIT_2",
                "BIT_ERROR_BASE_BIT_3",
                "BIT_ERROR_BASE",
                "BIT_ERROR_BASE_FAN",
                "BIT_ERROR_FRONTAL",
                "BIT_ERROR_PIOLTE",
                "BIT_ERROR_EXTERNAL_OLED",
                "BIT_ERROR_RTC",
                "BIT_ERROR_SD_CARD",
                "BIT_ERROR_ISM",
                "BIT_ERROR_MODBUS",
                "BIT_ERROR_INTERNAL_FAN",
                "BIT_ERROR_TIME_SYNC",
                "BIT_ERROR_CONSIGNE"
            ]
        else:
            values = [status_value]
            status_labels = None

        return SensorReading(device_id, values, labels=status_labels, index="S")

    @staticmethod
    def _states_reading(parsed_data: Dict[str, Any]) -> SensorReading:
        device_id = str(parsed_data["id"])
        values = parsed_data["states"]
        
        # States labels in the same order as the states array
        states_labels = [
            "LIGHT_ON_CH1",
            "LIGHT_ON_CH2",
            "LIGHT_OFF_CH1",
            "LIGHT_OFF_CH2",
            "LIGHT_FORCED_CH1",
            "LIGHT_FORCED_CH2",
            "LIGHT_ERR_CH1",
            "LIGHT_ERR_CH2"
        ]
        
        return SensorReading(device_id, values, labels=states_labels, index="E")

    @staticmethod
    def _reg_typ_reading(parsed_data: Dict[str, Any]) -> SensorReading:
        device_id = str(parsed_data["id"])
        reg_typ = parsed_data["reg_typ"]
        # Example bits: 2 -> TEC, 3 -> LIGHT
        values = [bool(reg_typ & (1 << 2)), bool(reg_typ & (1 << 3))]
        metadata = {
            "hw_type": parsed_data["hw_type"],
            "raw_reg_typ": reg_typ
        }
        
        # RegType labels in the same order as the values are extracted [TEC, LIGHT]
        regtype_labels = [
            "IS_TEC",
            "IS_LIGHT"
        ]
        
        return SensorReading(device_id, values, labels=regtype_labels, metadata=metadata, index="ST")

    @staticmethod
    def _all_channel_readings(parsed_data: Dict[str, Any], config: dict) -> List[dict]:
        """
        Processes all channels and groups values by index across ALL channels.
        Creates one SensorReading per unique index across all channels.
        Enhanced with hard-coded sensor specifications for auto-discovery:
        - Temperature (T): -30 to 60°C
        - Humidity (U): 0 to 100%
        """

        device_id = str(parsed_data["id"])
        channels = parsed_data["channels"]
        
        # Group values by their index across ALL channels
        index_groups = {}
        
        for channel_index, channel_data in enumerate(channels):
            values = channel_data.get("values", [])
            
            # Derive sensor type from config
            ch_type = channel_data["type"]
            sensor_type = config["sen_typ"].get(str(ch_type), "")

            # Flags
            reg_t = channel_data.get("reg_T", False)
            reg_u = channel_data.get("reg_U", False)
            ext_tu = channel_data.get("external_TU", False)
            
            for i, val in enumerate(values):
                # If sensor_type is 2 letters (e.g. "TU"), map each letter to the corresponding value
                if len(sensor_type) > 1:
                    index = sensor_type[i]
                else:
                    index = sensor_type  # single letter or empty

                # Determine label suffix
                if ext_tu:
                    suffix = "ext"
                else:
                    # For internal or regulated
                    if index == "T":
                        suffix = "reg" if reg_t else "int"
                    elif index == "U":
                        suffix = "reg" if reg_u else "int"
                    else:
                        suffix = "int"  # fallback

                # Group by index across all channels
                if index not in index_groups:
                    index_groups[index] = {
                        "values": [],
                        "labels": [],
                        "display_names": [],
                        "channel_metadata": [],
                        # Auto-discovery metadata - hard-coded sensor specifications
                        "auto_discovery_meta": {
                            "units": [],
                            "data_types": [],
                            "min_values": [],
                            "max_values": []
                        }
                    }
                
                index_groups[index]["values"].append(val)
                index_groups[index]["labels"].append(f"{index}_{suffix}" if index else None)
                index_groups[index]["display_names"].append(f"CH{channel_index+1} %name% {suffix}")
                
                # Store channel-specific metadata for this value
                channel_metadata = {
                    "hw_type": parsed_data["hw_type"],
                    "sen_typ": parsed_data["sen_typ"],
                    "channel_raw_data": parsed_data[f"CH{channel_index+1}_DATA"],
                    "channel_info": {k: v for k, v in channel_data.items() if k != "values"},
                    "channel_num": channel_index + 1
                }
                index_groups[index]["channel_metadata"].append(channel_metadata)
                
                # Auto-discovery metadata - hard-coded sensor specifications
                auto_meta = index_groups[index]["auto_discovery_meta"]
                
                # Hard-code metadata based on sensor type (parser creator defines specifications)
                if index == "T":  # Temperature sensor
                    auto_meta["units"].append("°C")
                    auto_meta["data_types"].append("number")
                    auto_meta["min_values"].append(-30.0)  # Known temperature range
                    auto_meta["max_values"].append(60.0)
                elif index == "U":  # Humidity sensor
                    auto_meta["units"].append("%")
                    auto_meta["data_types"].append("number")
                    auto_meta["min_values"].append(0.0)    # Known humidity range
                    auto_meta["max_values"].append(100.0)
                else:  # Other sensors - default to number type
                    auto_meta["units"].append("")  # No unit specified
                    auto_meta["data_types"].append("number")
                    auto_meta["min_values"].append(None)   # No known range
                    auto_meta["max_values"].append(None)

        # Create one SensorReading per unique index

        reading_list = []
        for index, group_data in index_groups.items():
            # Calculate aggregated ranges for auto-discovery
            auto_meta = group_data["auto_discovery_meta"]
            min_vals = [v for v in auto_meta["min_values"] if v is not None]
            max_vals = [v for v in auto_meta["max_values"] if v is not None]
            
            # Add suggested min/max constraints
            if min_vals:
                # Use the hard-coded min value (should be consistent for same sensor type)
                auto_meta["suggested_min"] = min_vals[0]
            if max_vals:
                # Use the hard-coded max value (should be consistent for same sensor type)
                auto_meta["suggested_max"] = max_vals[0]
            
            # Add suggested units (remove empty strings)
            specified_units = [unit for unit in auto_meta["units"] if unit]
            if specified_units:
                auto_meta["suggested_unit"] = specified_units
            
            # Add suggested data types (remove duplicates)
            specified_types = list(set(auto_meta["data_types"]))
            if specified_types:
                auto_meta["suggested_data_type"] = specified_types
            
            reading_list.append(
                asdict(SensorReading(
                    device_id=device_id,
                    values=group_data["values"],
                    labels=group_data["labels"],
                    display_names=group_data["display_names"],
                    index=index,
                    metadata={
                        "channels": group_data["channel_metadata"],
                        "auto_discovery": auto_meta  # Simplified metadata for auto-discovery
                    }
                ))
            )

        return reading_list

    @staticmethod
    def normalize(parsed_data: Dict[str, Any], config: dict) -> List[dict]:
        """
        Produces a list of SensorReading objects from the entire parsed_data.
        """
        readings = []
        # Settings - now returns a list of SensorReadings
        readings.append(asdict(Normalizer._settings_reading(parsed_data)))
        # Control
        readings.append(asdict(Normalizer._control_reading(parsed_data)))
        # Status
        readings.append(asdict(Normalizer._status_reading(parsed_data)))
        # States
        readings.append(asdict(Normalizer._states_reading(parsed_data)))
        # reg_typ
        readings.append(asdict(Normalizer._reg_typ_reading(parsed_data)))
        # Channels - process all channels together and group by index
        readings.extend(Normalizer._all_channel_readings(parsed_data, config))

        return readings

# ---------------------------------------------------------
# Helper function for end-to-end decoding + normalization
# ---------------------------------------------------------
def extract_broker_id(source_topic: str) -> str:
    """
    Extract broker ID from source topic.
    
    Args:
        source_topic: The MQTT topic string, e.g., "broker_data/2121000/2121004/data"
    
    Returns:
        str: The broker ID (e.g., "2121000")
    
    Raises:
        ValueError: If the topic format is invalid
    """
    if not source_topic:
        raise ValueError("Source topic cannot be empty")
    
    parts = source_topic.split('/')
    if len(parts) < 2 or parts[0] != 'broker_data':
        raise ValueError(f"Invalid topic format. Expected 'broker_data/<broker_id>/...' but got '{source_topic}'")
    
    broker_id = parts[1]
    if not broker_id:
        raise ValueError(f"Broker ID is empty in topic: {source_topic}")
    
    return broker_id

def safe_hex_decode(hex_string):
    """Safely decode hex string to bytes."""
    if not hex_string:
        return None
    
    try:
        return bytes.fromhex(hex_string)
    except ValueError as e:
        print(f"Invalid hex string '{hex_string}': {e}")
        return None

def parse_feedback(payload: any, metadata: dict, config: Optional[dict] = None, message_parser: Optional[Any] = None) -> Tuple[str, dict]:
    """Parse feedback for Fluxor devices using proprietary message_parser."""
    device_id = metadata.get('device_id')
    logger.info(f"parse_feedback payload: {payload}")
    # payload_bytes = safe_hex_decode(payload)
    # logger.info(f"payload_bytes: {payload_bytes}")

    
    # If payload is numeric (either int or numeric string), return None
    if isinstance(payload, int):
        return None, None
    if isinstance(payload, str):
        try:
            int(payload)
            return None, None
        except ValueError:
            pass
    payload = json.loads(payload)
    uid = payload.get('uid')
    uid_parts = uid.split('-')
    sensor_id = uid_parts[0]
    command_id = '-'.join(uid_parts[:-1])  # Everything except the last part
    parsed_device_id = uid_parts[-1]       # The last part
    if parsed_device_id != device_id:
        logger.warning(f"Device ID mismatch: {parsed_device_id} != {device_id}")
        return None, None
    
    status = "success" if payload.get('status') == 1 else "error"
    payload = {
        
        "status": status,
        "request_id": command_id,
    }
    data = {
        "device_id": sensor_id,
        "command_id": command_id,
        "payload": payload,
        "metadata": metadata
    }
    logger.info(f"Fluxor feedback: {data}")
    return "feedback", data


def parse(payload: str, metadata: dict, config: dict, message_parser: Optional[Any] = None) -> Tuple[str, List[dict]]:
    """
    Convenience function that decodes a hex payload and then normalizes
    it into a list of SensorReading objects.
    
    Args:
        payload: Raw payload bytes
        metadata: Metadata dictionary from the raw message
        config: Hardware configuration dictionary
        message_parser: Optional message parser instance (not used in this proprietary parser)
    
    Returns:
        List of dictionaries matching StandardizedOutput structure
    """
    # Note: message_parser is not used in this proprietary parser
    # This parser handles its own decoding logic for the Fluxor format
    payload_bytes = binascii.unhexlify(payload)
    # payload_bytes = binascii.unhexlify(payload)
    parsed_data = Decoder.decode(payload_bytes, config)
    normalized_data = Normalizer.normalize(parsed_data, config)
    return "data", normalized_data

# ---------------------------------------------------------
# Command Formatting Functions
# ---------------------------------------------------------
def _format_fluxor_config_command(command_id: str, device_id: str, payload: dict, metadata: dict) -> bytes:
    """Format config command for Fluxor devices using proprietary message_parser."""
    logger.info(f"Formatting Fluxor config command: {payload} for device {device_id}")
    
    # Extract input values with defaults
    t_consigne = payload.get('T_consigne', 30.50)
    t_hysteresys = payload.get('T_hysteresys', 0)
    u_consigne = payload.get('U_consigne')
    u_hysteresys = payload.get('U_hysteresys')
    is_sensor_active = payload.get('reg_type_state')
    
    # Extract metadata values
    base_reg_type = metadata.get('reg_type')
    hw_type = metadata.get('hw_type')
    sen_type = metadata.get('sen_typ')
    mqtt_topic = metadata.get('mqtt_topic', '')
    
    # Calculate reg_type based on state transitions
    current_active = bool(base_reg_type & (1 << 14))
    
    # If currently inactive but user wants to activate
    if not current_active and is_sensor_active:
        reg_type = base_reg_type + (1 << 14) + (1 << 15)
    # If currently active but user wants to deactivate  
    elif current_active and not is_sensor_active:
        reg_type = base_reg_type - (1 << 14) + (1 << 15)
    # No change in activation state
    else:
        reg_type = base_reg_type + (1 << 15)
    
    # Extract broker ID from topic
    broker_id = extract_broker_id(mqtt_topic)
    
    # Build command payload with proper transformations
    command_payload = {
        "id_capteur": device_id,
        "hw_type": hw_type,
        "reg_type": reg_type,
        "sen_type": sen_type,
        "U_consigne": int(u_consigne * 100),
        "U_hysteresys": round(u_hysteresys * 10 + 127),
        "T_consigne": int(t_consigne * 100),
        "T_hysteresys": round(t_hysteresys * 10 + 127),
        "T_int_offset": 127,
        "U_int_offset": 127,
        "T_ext_offset": 127,
        "U_ext_offset": 127,
        "control": 0,
        "uid": f"{command_id}-{broker_id}"
    }
    
    # Build final command structure
    command = {
        "payload": command_payload,
        "metadata": {
            "device_id": device_id,
            "broker_id": broker_id,
            "topic": f"broker_data/{broker_id}/command",
            "qos": 0,
            "retain": False
        }
    }
    
    return command

def _format_fluxor_reboot_command(command_id: str, device_id: str, payload: dict, metadata: dict) -> bytes:
    """Format reboot command for Fluxor devices using proprietary message_parser."""
    logger.info(f"Formatting Fluxor reboot command: {payload} for device {device_id}")
    # Extract input values with defaults
    t_consigne = payload.get('T_consigne', 30.50)
    t_hysteresys = payload.get('T_hysteresys', 0)
    u_consigne = payload.get('U_consigne')
    u_hysteresys = payload.get('U_hysteresys')
    
    # Extract metadata values
    reg_type = metadata.get('reg_type')
    hw_type = metadata.get('hw_type')
    sen_type = metadata.get('sen_typ')
    mqtt_topic = metadata.get('mqtt_topic', '')
    control = 1 << 13
    
    # Extract broker ID from topic
    broker_id = extract_broker_id(mqtt_topic)
    
    # Build command payload with proper transformations
    command_payload = {
        "id_capteur": device_id,
        "hw_type": hw_type,
        "reg_type": reg_type,
        "sen_type": sen_type,
        "U_consigne": int(u_consigne * 100),
        "U_hysteresys": round(u_hysteresys * 10 + 127),
        "T_consigne": int(t_consigne * 100),
        "T_hysteresys": round(t_hysteresys * 10 + 127),
        "T_int_offset": 127,
        "U_int_offset": 127,
        "T_ext_offset": 127,
        "U_ext_offset": 127,
        "control": control,
        "uid": f"{command_id}-{broker_id}"
    }
    
    # Build final command structure
    command = {
        "payload": command_payload,
        "metadata": {
            "device_id": device_id,
            "broker_id": broker_id,
            "topic": f"broker_data/{broker_id}/command",
            "qos": 0,
            "retain": False
        }
    }
    
    return command

def _format_fluxor_synchronize_command(command_id: str, device_id: str, payload: dict, metadata: dict) -> bytes:
    """Format synchronize command for Fluxor devices using proprietary message_parser."""
    logger.info(f"Formatting Fluxor synchronize command: {payload} for device {device_id}")
    
    # Extract mqtt_topic from metadata
    mqtt_topic = metadata.get('mqtt_topic', '')
    
    # Extract broker ID from topic
    broker_id = extract_broker_id(mqtt_topic)
    
    # Build sync time command payload
    command_payload = {
        "id_capteur": int(device_id),
        "epoch": int(time.time()),
        "sync_header": 0,
        "sync_alarm": 0,
        "uid": f"{command_id}-{broker_id}"
    }
    
    # Build final command structure
    command = {
        "payload": command_payload,
        "metadata": {
            "device_id": device_id,
            "broker_id": broker_id,
            "topic": f"broker_data/{broker_id}/command",
            "qos": 0,
            "retain": False
        }
    }
    
    return command


def format_command(command: dict, config: dict, message_parser: Optional[Any] = None) -> str:
    """Format commands for Fluxor devices using proprietary message_parser."""
    logger.debug(f"Formatting Fluxor command: {command.get('command_type')} for device {command.get('device_id')}")
    
    command_type = command.get('command_type')
    payload = command.get('payload', {})
    metadata = command.get('metadata', {})
    device_id = command.get('device_id')
    command_id = command.get('request_id')
    formatters = {
        "config": lambda: _format_fluxor_config_command(command_id, device_id, payload, metadata),
        "reboot": lambda: _format_fluxor_reboot_command(command_id, device_id, payload, metadata),
        "synchronize": lambda: _format_fluxor_synchronize_command(command_id, device_id, payload, metadata)
    }
    
    formatter = formatters.get(command_type)
    if not formatter:
        raise ValueError(f"Unsupported command type for Fluxor device: {command_type}")
    
    return formatter()

# ---------------------------------------------------------
# Example usage
# ---------------------------------------------------------
if __name__ == "__main__":
    test_payload = b'19ad210000484443830000e0157d00007f7f7f7f7f662cd815742cce190000000000000000000000000000'
    all_readings = parse(test_payload)

    for i, reading in enumerate(all_readings, 1):
        print(f"\n=== Reading {i} ===")
        print(json.dumps(reading, indent=4, cls=DateTimeEncoder))
