# storage/parser_scripts/light_parser.py
import binascii
import struct
import json
from typing import List, Dict, Any, Tuple, Optional, NamedTuple
from dataclasses import asdict, dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Header(NamedTuple):
    """Immutable header structure for better performance and type safety."""
    task_id: int
    reserved: int
    all: int
    clear: int
    get_set: int

class Alarm(NamedTuple):
    """Immutable alarm structure for better performance and type safety."""
    time_beg: int
    duration: int
    day: int
    task_id: int
    channel: int
    active: int
    run_once: int

@dataclass(slots=True, frozen=True)
class SensorReading:
    """Optimized with slots for memory efficiency and frozen for immutability."""
    device_id: str
    values: Tuple[Any, ...]  # Tuple for immutability and memory efficiency
    labels: Optional[Tuple[str, ...]] = None
    display_names: Optional[Tuple[str, ...]] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    index: str = "L"

class LightProtocolDecoder:
    """Optimized decoder with cached operations and better error handling."""
    
    # Pre-computed constants for bit operations
    _HEADER_MASKS = {
        'task_id': 0x0F,
        'reserved': 0x01,
        'all': 0x01,
        'clear': 0x01,
        'get_set': 0x01
    }
    
    _ALARM_MASKS = {
        'time_beg': 0x7FF,
        'duration': 0x7FF,
        'day': 0x07,
        'task_id': 0x0F,
        'channel': 0x01,
        'active': 0x01,
        'run_once': 0x01
    }
    
    # Operation lookup table for O(1) determination
    _OPERATIONS = {
        (0, 0, 0): 'read_task',
        (0, 1, 0): 'read_all',
        (1, 0, 0): 'create_task',
        (1, 0, 1): 'delete_task',
        (1, 1, 1): 'delete_all'
    }
    
    @classmethod
    def decode_packet(cls, binary_data: str) -> Dict[str, Any]:
        """Optimized packet decoding with better error handling."""
        if not binary_data or len(binary_data) < 13:  # 13 bytes = 26 hex chars
            raise ValueError(f"Invalid packet length: {len(binary_data)//2} bytes (minimum 13 required)")
        
        # Optimized unpacking in single operation - LITTLE ENDIAN!
        id_capteur, epoch, sync_header, sync_alarm = struct.unpack('=IIBI', binary_data)
        
        # Optimized bit extraction using pre-computed masks
        header = Header(
            task_id=sync_header & cls._HEADER_MASKS['task_id'],
            reserved=(sync_header >> 4) & cls._HEADER_MASKS['reserved'],
            all=(sync_header >> 5) & cls._HEADER_MASKS['all'],
            clear=(sync_header >> 6) & cls._HEADER_MASKS['clear'],
            get_set=(sync_header >> 7) & cls._HEADER_MASKS['get_set']
        )
        
        alarm = Alarm(
            time_beg=sync_alarm & cls._ALARM_MASKS['time_beg'],
            duration=(sync_alarm >> 11) & cls._ALARM_MASKS['duration'],
            day=(sync_alarm >> 22) & cls._ALARM_MASKS['day'],
            task_id=(sync_alarm >> 25) & cls._ALARM_MASKS['task_id'],
            channel=(sync_alarm >> 29) & cls._ALARM_MASKS['channel'],
            active=(sync_alarm >> 30) & cls._ALARM_MASKS['active'],
            run_once=(sync_alarm >> 31) & cls._ALARM_MASKS['run_once']
        )
        
        # Fast operation lookup
        operation_key = (header.get_set, header.all, header.clear)
        operation = cls._OPERATIONS.get(operation_key, 'unknown')
        
        return {
            'id_capteur': id_capteur,
            'epoch': epoch,
            'header': header._asdict(),
            'alarm': alarm._asdict(),
            'operation': operation
        }
    
    @classmethod
    def encode_packet(cls, data: Dict[str, Any], format: str = "dict") -> str:
        """Optimized packet encoding with validation."""
        header = data.get("header", {})
        alarm = data.get("alarm", {})

        # Optimized bit packing with bounds checking
        sync_header = (
            (header.get("task_id", 0) & cls._HEADER_MASKS["task_id"])
            | ((header.get("reserved", 0) & cls._HEADER_MASKS["reserved"]) << 4)
            | ((header.get("all", 0) & cls._HEADER_MASKS["all"]) << 5)
            | ((header.get("clear", 0) & cls._HEADER_MASKS["clear"]) << 6)
            | ((header.get("get_set", 0) & cls._HEADER_MASKS["get_set"]) << 7)
        )

        sync_alarm = (
            (alarm.get("time_beg", 0) & cls._ALARM_MASKS["time_beg"])
            | ((alarm.get("duration", 0) & cls._ALARM_MASKS["duration"]) << 11)
            | ((alarm.get("day", 0) & cls._ALARM_MASKS["day"]) << 22)
            | ((alarm.get("task_id", 0) & cls._ALARM_MASKS["task_id"]) << 25)
            | ((alarm.get("channel", 0) & cls._ALARM_MASKS["channel"]) << 29)
            | ((alarm.get("active", 0) & cls._ALARM_MASKS["active"]) << 30)
            | ((alarm.get("run_once", 0) & cls._ALARM_MASKS["run_once"]) << 31)
        )

        if format == "dict":
            return {
                "id_capteur": data.get("id_capteur", 0),
                "epoch": data.get("epoch", int(datetime.now().timestamp())),
                "sync_header": sync_header,
                "sync_alarm": sync_alarm,
            }
        elif format == "hex":
            # Single pack operation - LITTLE ENDIAN!
            binary_data = struct.pack(
                "=IIBI",
                data.get("id_capteur", 0),
                data.get("epoch", int(datetime.now().timestamp())),
                sync_header,
                sync_alarm,
            )
            return binary_data.hex()
        else:
            raise ValueError(f"Invalid format: {format}. Must be 'hex' or 'dict'")
          

def safe_hex_decode(hex_string):
    """Safely decode hex string to bytes."""
    if not hex_string:
        return None
    
    try:
        return bytes.fromhex(hex_string)
    except ValueError as e:
        print(f"Invalid hex string '{hex_string}': {e}")
        return None
    
def parse(payload: str, metadata: dict, config: dict, message_parser=None) -> Tuple[str, List[dict]]:
    """Optimized main parser with better error handling."""
    device_id = metadata.get('device_id', 'unknown')

    try:
        binary_data = binascii.unhexlify(payload)
        decoded = LightProtocolDecoder.decode_packet(binary_data)
        operation = decoded['operation']
        logger.info(f"decoded: {decoded}")
        # Only create readings for relevant operations
        if operation in ('read_all', 'create_task'):
            reading = _create_task_reading(device_id, decoded, metadata)
            return ("data", [reading] if reading else [])
        
        return ("data", [])
    
    except Exception as e:
        logger.error(f"Error parsing light message from {device_id}: {e}")
        return ("data", [])

def _create_task_reading(device_id: str, decoded: Dict[str, Any], metadata: dict) -> Optional[dict]:
    """Optimized task reading creation with cached string formatting."""
    
    alarm = decoded['alarm']
    task_id = alarm['task_id']
    
    # Pre-build task prefix for efficiency
    task_prefix = f"task_{task_id}"
    
    # Optimized tuple creation (immutable and faster)
    values = (
        alarm['time_beg'],
        alarm['duration'], 
        alarm['day'],
        alarm['channel'],
        alarm['active'],
        alarm['run_once']
    )
    
    # Optimized label generation with list comprehension
    base_labels = ('time_beg', 'duration', 'day', 'channel', 'active', 'run_once')
    labels = tuple(f"{task_prefix}_{label}" for label in base_labels)
    
    # Optimized display names with precomputed task display
    task_display = f"Tâche {task_id}"
    display_suffixes = ('- Heure début', '- Durée (min)', '- Jour', '- Canal', '- Actif', '- Unique')
    display_names = tuple(f"{task_display} {suffix}" for suffix in display_suffixes)
    
    reading = SensorReading(
        device_id=device_id,
        values=values,
        labels=labels,
        display_names=display_names,
        timestamp=datetime.now(),
        metadata={
            'operation': decoded['operation'],
            'task_id': task_id,
            **metadata
        },
        index="L"
    )
    
    return asdict(reading)

# Command formatting functions optimized for reusability
class CommandFormatter:
    """Centralized command formatting with template reuse."""
    
    @staticmethod
    def _build_base_packet(device_id: str, header_params: Dict[str, int], 
                          alarm_params: Dict[str, int]) -> Dict[str, Any]:
        """Shared packet building logic."""
        return {
            'id_capteur': int(device_id),
            'epoch': int(datetime.now().timestamp()),
            'header': {
                'task_id': header_params.get('task_id', 0),
                'reserved': 0,
                'all': header_params.get('all', 0),
                'clear': header_params.get('clear', 0),
                'get_set': header_params.get('get_set', 0)
            },
            'alarm': {
                'time_beg': alarm_params.get('time_beg', 0),
                'duration': alarm_params.get('duration', 0),
                'day': alarm_params.get('day', 0),
                'task_id': alarm_params.get('task_id', 0),
                'channel': alarm_params.get('channel', 0),
                'active': alarm_params.get('active', 0),
                'run_once': alarm_params.get('run_once', 0)
            }
        }
    
    @staticmethod
    def _encode_and_wrap(packet_data: Dict[str, Any], command_type: str) -> str:
        """Shared encoding and wrapping logic."""
        encoded_hex = LightProtocolDecoder.encode_packet(packet_data)
        # mqtt_payload = {
        #     'data': encoded_hex,
        #     'type': command_type,
        #     'timestamp': packet_data['epoch']
        # }
        # return json.dumps(mqtt_payload, separators=(',', ':'))  # Compact JSON
        return encoded_hex

def format_command(command: dict, config: dict, message_parser=None) -> str:
    """Optimized command dispatcher with lookup table."""
    command_type = command.get('command_type', '')
    
    # Command handler lookup table for O(1) dispatch
    handlers = {
        'light_config': _format_light_config_command,
        'light_read_all': _format_read_all_command, 
        'light_delete_task': _format_delete_task_command,
        'light_delete_all': _format_delete_all_command
    }
    
    handler = handlers.get(command_type)
    if not handler:
        raise ValueError(f"Unknown light command type: {command_type}")
        
    payload = handler(command, config)
    metadata = command.get('metadata', {})
    broker_id = metadata.get('broker')
    command_id = command.get('request_id')
    command = {
        "payload": {"uid": f"{command_id}-{broker_id}", **payload},
        "metadata": {
            "device_id": command['device_id'],
            "broker_id": broker_id,
            "topic": f"broker_data/{broker_id}/command",
            "qos": 0,
            "retain": False
        }
    }
    logger.info(f"command: {command}")
    return command

def _format_light_config_command(command: dict, config: dict) -> str:
    """Optimized config command formatting."""
    payload = command['payload']
    task_id = payload.get('task_id', 0)
    
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'],
        {'task_id': task_id, 'get_set': 1},  # Write operation
        {
            'time_beg': payload.get('time_beg', 0),
            'duration': payload.get('duration', 0),
            'day': payload.get('day', 0),
            'task_id': task_id,
            'channel': payload.get('channel', 0),
            'active': payload.get('active', 1),
            'run_once': payload.get('run_once', 0)
        }
    )
    
    return CommandFormatter._encode_and_wrap(packet_data, 'light_config')

def _format_read_all_command(command: dict, config: dict) -> str:
    """Optimized read all command formatting."""
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'],
        {'all': 1, 'get_set': 0},  # Read all
        {}
    )
    logger.info(f"packet_data: {packet_data}")
    return CommandFormatter._encode_and_wrap(packet_data, 'light_read_all')

def _format_delete_task_command(command: dict, config: dict) -> str:
    """Optimized delete task command formatting."""
    task_id = command['payload'].get('task_id', 0)
    
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'],
        {'task_id': task_id, 'clear': 1, 'get_set': 1},  # Delete specific
        {'task_id': task_id}
    )
    
    return CommandFormatter._encode_and_wrap(packet_data, 'light_delete_task')

def _format_delete_all_command(command: dict, config: dict) -> str:
    """Optimized delete all command formatting."""
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'], 
        {'all': 1, 'clear': 1, 'get_set': 1},  # Delete all
        {}
    )
    
    return CommandFormatter._encode_and_wrap(packet_data, 'light_delete_all')