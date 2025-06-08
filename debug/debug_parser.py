import binascii
import json
import datetime
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Any, Optional

# ---------------------------------------------------------
# Configuration & JSON Encoder
# ---------------------------------------------------------
config = {
    "sentyp": {
        3: "TU",
        2: "TU",
        # Add other sensor types here if needed
    }
}
parsing_op = {
    3: ("(%val%-10000)/100","(%val%/100)"),
    2: ("(%val%-10000)/100","(%val%/100)"),
}

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
    def _extract_channel_data(sen_typ: int, raw_data: List[int]) -> List[Dict[str, Any]]:
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
            sensor_type = config["sentyp"].get(ch_type, "")
            low = data_raw & 0xFFFF
            high = (data_raw >> 16) & 0xFFFF
            
            
            
            if sensor_type:  # recognized
                # For 2-letter sensor (e.g. "TU"), decode as 2 separate values
                if len(sensor_type) > 1:
                    formula_low = parsing_op[ch_type][0].replace("%val%", str(low))
                    low = eval(formula_low, {"__builtins__": {}})
                    formula_high = parsing_op[ch_type][1].replace("%val%", str(high))
                    high = eval(formula_high, {"__builtins__": {}})
                    decoded["values"] = [low, high]
                else:
                    formula_low = parsing_op[ch_type][0].replace("%val%", str(low + high))
                    val = eval(formula_low, {"__builtins__": {}})
                    decoded["values"] = [val]
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
    def decode(payload_hex: str) -> Dict[str, Any]:
        """
        Decodes the given hex payload into a Python dictionary with
        meaningful fields.
        """
        data = binascii.unhexlify(payload_hex)

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
        d["channels"] = Decoder._extract_channel_data(d["sen_typ"], ch_data)

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
    label: Optional[List[str]] = field(default=None)
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
        1. One for consigne values (index "C")
        2. One for hysteresys values (index "H")
        """
        device_id = str(parsed_data["id"])
        # Consigne reading
        consigne_keys = ["U_consigne", "T_consigne"]
        consigne_values = [parsed_data[k] for k in consigne_keys]
        consigne_reading = SensorReading(
            device_id=device_id, 
            values=consigne_values, 
            index="C"
        )
        
        # Hysteresys reading
        hysteresys_keys = ["U_hysteresys", "T_hysteresys"]
        hysteresys_values = [parsed_data[k] for k in hysteresys_keys]
        hysteresys_reading = SensorReading(
            device_id=device_id, 
            values=hysteresys_values, 
            index="H"
        )
        
        return [asdict(consigne_reading), asdict(hysteresys_reading)]

    @staticmethod
    def _control_reading(parsed_data: Dict[str, Any]) -> SensorReading:
        device_id = str(parsed_data["id"])
        values = parsed_data["control"]
        metadata = {}

        # Optionally store MEM_VA
        if "MEM_VA" in parsed_data:
            metadata["MEM_VA"] = parsed_data["MEM_VA"]

        return SensorReading(device_id, values, metadata=metadata, index="CTRL")

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
        else:
            values = [status_value]

        return SensorReading(device_id, values, index="S")

    @staticmethod
    def _states_reading(parsed_data: Dict[str, Any]) -> SensorReading:
        device_id = str(parsed_data["id"])
        values = parsed_data["states"]
        return SensorReading(device_id, values, index="E")

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
        return SensorReading(device_id, values, metadata=metadata, index="ST")

    @staticmethod
    def _channel_readings(parsed_data: Dict[str, Any], channel_index: int) -> List[dict]:
        """
        Creates one or more SensorReading for a specific channel.
        """
        channels = parsed_data["channels"]
        if channel_index < 0 or channel_index >= len(channels):
            raise ValueError(f"Invalid channel index: {channel_index}")

        channel_data = channels[channel_index]
        device_id = str(parsed_data["id"])
        values = channel_data.get("values", [])

        # Prepare metadata
        channel_metadata = {
            "hw_type": parsed_data["hw_type"],
            "sen_typ": parsed_data["sen_typ"],
            "channel_raw_data": parsed_data[f"CH{channel_index+1}_DATA"],
            "channel_info": {k: v for k, v in channel_data.items() if k != "values"},
        }

        # Derive sensor type from config
        ch_type = channel_data["type"]
        sensor_type = config["sentyp"].get(ch_type, "")

        # Flags
        reg_t = channel_data.get("reg_T", False)
        reg_u = channel_data.get("reg_U", False)
        ext_tu = channel_data.get("external_TU", False)

        # Build a reading per value
        reading_list = []
        for i, val in enumerate(values):
            # device_id = f"{base_device_id}_CH{channel_data['channel_num']}_V{i+1}"
            # If sensor_type is 2 letters (e.g. "TU"), map each letter to the corresponding value
            if len(sensor_type) > 1:
                index = sensor_type[i]
            else:
                index = sensor_type  # single letter or empty

            # Determine label suffix
            if ext_tu:
                suffix = "_ext"
            else:
                # For internal or regulated
                if index == "T":
                    suffix = "_reg" if reg_t else "_int"
                elif index == "U":
                    suffix = "_reg" if reg_u else "_int"
                else:
                    suffix = "_int"  # fallback

            label = [f"{index}{suffix}"] if index else None

            reading_list.append(
                asdict(SensorReading(
                    device_id=device_id,
                    values=[val],
                    label=label,
                    index=index,
                    metadata=channel_metadata.copy()
                ))
            )

        return reading_list

    @staticmethod
    def normalize(parsed_data: Dict[str, Any]) -> List[dict]:
        """
        Produces a list of SensorReading objects from the entire parsed_data.
        """
        readings = []
        # Settings - now returns a list of SensorReadings
        # for reading in Normalizer._settings_reading(parsed_data):
        #     readings.append(reading)
        # Control
        # readings.append(asdict(Normalizer._control_reading(parsed_data)))
        # Status
        # readings.append(asdict(Normalizer._status_reading(parsed_data)))
        # States
        # readings.append(asdict(Normalizer._states_reading(parsed_data)))
        # reg_typ
        # readings.append(asdict(Normalizer._reg_typ_reading(parsed_data)))
        # Channels
        for i in range(len(parsed_data["channels"])):
            readings.extend(Normalizer._channel_readings(parsed_data, i))

        return readings

# ---------------------------------------------------------
# Helper function for end-to-end decoding + normalization
# ---------------------------------------------------------
def parse(payload: bytes, config: dict = None) -> List[dict]:
    """
    Convenience function that decodes a hex payload and then normalizes
    it into a list of SensorReading objects.
    """
    parsed_data = Decoder.decode(payload.decode())
    return Normalizer.normalize(parsed_data)

# ---------------------------------------------------------
# Example usage
# ---------------------------------------------------------
if __name__ == "__main__":
    # test_payload = b'19ad210000484443830000e0157d00007f7f7f7f7f662cd815742cce190000000000000000000000000000'
    # test_payload = bytes.fromhex('3931393932313030303034323464343330303030303035303134376530303030376637663766376637666334326630383134303030303030303030303030303030303030303030303030303230303030303030303030')
    # all_readings = parse(test_payload)
    # # print(json.dumps(all_readings[-1], indent=4, cls=DateTimeEncoder))
    # for i, reading in enumerate(all_readings, 1):
    #     print(f"\n=== Reading {i} ===")
    #     print(json.dumps(reading, indent=4, cls=DateTimeEncoder))
    # Load debug messages from JSON file
    with open('debug/debug_messages.json', 'r') as f:
        debug_messages = json.load(f)
    
    print(f"Processing {len(debug_messages)} debug messages...")
    
    # Dictionary to collect data by device_id
    device_data = {}
    
    for msg_idx, message in enumerate(debug_messages, 1):
        device_id = message['device_id']
        payload_bytes = bytes.fromhex(message['payload_hex'])
        timestamp = message['timestamp']
        
        print(f"Processing message {msg_idx}/{len(debug_messages)} for device {device_id}")
        
        try:
            # Decode the payload to get raw parsed data
            parsed_data = Decoder.decode(payload_bytes)
            
            # Initialize device data if not exists
            if device_id not in device_data:
                device_data[device_id] = {}
            
            # Extract data from channels
            for channel in parsed_data.get("channels", []):
                ch_type = channel["type"]
                values = channel.get("values", [])
                
                # Get sensor type from config
                sensor_type = config["sentyp"].get(ch_type, "")
                
                # Get parsing operations for this channel type
                if ch_type in parsing_op:
                    parsing_formulas = parsing_op[ch_type]
                    
                    # Process each value in the channel
                    for i, value in enumerate(values):
                        # Determine label based on sensor type and channel info
                        if len(sensor_type) > 1:
                            # For 2-letter sensor types (e.g., "TU")
                            label_base = sensor_type[i] if i < len(sensor_type) else sensor_type[0]
                        else:
                            label_base = sensor_type
                        
                        # Add suffix based on channel flags
                        if channel.get("external_TU", False):
                            suffix = "_ext"
                        else:
                            if label_base == "T":
                                suffix = "_reg" if channel.get("reg_T", False) else "_int"
                            elif label_base == "U":
                                suffix = "_reg" if channel.get("reg_U", False) else "_int"
                            else:
                                suffix = "_int"
                        
                        label = f"CH{channel['channel_num']}_{label_base}{suffix}"
                        
                        # Get the appropriate parsing formula
                        if len(sensor_type) > 1 and i < len(parsing_formulas):
                            formula = parsing_formulas[i]
                        else:
                            formula = parsing_formulas[0] if parsing_formulas else "(%val%)"
                        
                        # Add to device data (label -> parsing_op mapping)
                        device_data[device_id][label] = formula
                
        except Exception as e:
            print(f"ERROR parsing payload for device {device_id}: {e}")
    
    # Save device_data to JSON file
    with open('debug/mappings.json', 'w') as f:
        json.dump(device_data, f, indent=2, cls=DateTimeEncoder)
    
    print(f"\nSaved device mappings to debug/mappings.json")
    
    # Print final results
    print(f"\n{'='*80}")
    print("FINAL DEVICE DATA")
    print(f"{'='*80}")
    
    for device_id, data in device_data.items():
        print(f"\nDevice {device_id}:")
        print(json.dumps({device_id: data}, indent=2, cls=DateTimeEncoder))
    
    print(f"\nProcessed data for {len(device_data)} unique devices.")
