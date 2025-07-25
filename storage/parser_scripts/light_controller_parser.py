# storage/parser_scripts/light_parser.py
import binascii
import struct
import json
from typing import List, Dict, Any, Tuple, Optional, NamedTuple, Union
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
    values: Tuple[Any, ...]
    labels: Optional[Tuple[str, ...]] = None
    display_names: Optional[Tuple[str, ...]] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    index: str = "L"

class ParseResult(NamedTuple):
    """Type-safe parse result."""
    result_type: str
    readings: List[dict]

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
    def decode_packet(cls, binary_data: bytes) -> Dict[str, Any]:
        """Optimized packet decoding with better error handling."""
        if len(binary_data) < 13:
            raise ValueError(f"Invalid packet length: {len(binary_data)} bytes (minimum 13 required)")
        
        # Optimized unpacking in single operation - LITTLE ENDIAN!
        id_capteur, epoch, sync_header, sync_alarm = struct.unpack('=IIBI', binary_data[:13])
        
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
            'operation': operation,
            'sync_alarm': sync_alarm  # Keep for validation
        }
    
    @classmethod
    def encode_packet(cls, data: Dict[str, Any], format: str = "dict") -> Union[Dict[str, Any], str]:
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

class OperationHandler:
    """Handles different operation types with clear separation of concerns."""
    
    @staticmethod
    def handle_read_all(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Read All response - extract task count from sync_alarm."""
        task_count = decoded['alarm']['task_id']
        logger.info(f"Read All response: {task_count} tasks configured")
        
        # Validate sync_alarm value for Read All responses
        if decoded['header']['all'] == 1 and decoded['header']['get_set'] == 0:
            sync_alarm = decoded.get('sync_alarm', 0)
            if sync_alarm > 0:
                logger.debug(f"Valid Read All response with sync_alarm: {sync_alarm}")
        
        reading = SensorReading(
            device_id=device_id,
            values=(task_count,),
            labels=('task_count',),
            display_names=('Nombre de tâches',),
            timestamp=datetime.now(),
            metadata={
                'operation': decoded['operation'],
                'task_count': task_count,
                **metadata
            },
            index='L'
        )
        return [asdict(reading)]
    
    @staticmethod
    def handle_create_task(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle task creation/update operations."""
        reading = OperationHandler._create_task_reading(device_id, decoded, metadata)
        return [reading] if reading else []
    
    @staticmethod
    def _create_task_reading(device_id: str, decoded: Dict[str, Any], metadata: dict) -> Optional[dict]:
        """Create optimized task reading with cached string formatting."""
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
        
        # Optimized label generation
        base_labels = ('time_beg', 'duration', 'day', 'channel', 'active', 'run_once')
        labels = tuple(f"{task_prefix}_{label}" for label in base_labels)
        
        # Optimized display names
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
    
    @staticmethod
    def handle_read_task(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Read Single Task operation - returns specific task data."""
        # For read_task responses, parse the specific task data
        reading = OperationHandler._create_task_reading(device_id, decoded, metadata)
        return [reading] if reading else []
    
    @staticmethod
    def handle_delete_task(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Delete Task operation response."""
        # Delete operations typically don't return data, just acknowledgment
        logger.info(f"Delete task operation completed for device {device_id}")
        return []
    
    @staticmethod
    def handle_delete_all(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Delete All Tasks operation response."""
        # Delete all operations typically don't return data, just acknowledgment
        logger.info(f"Delete all tasks operation completed for device {device_id}")
        return []

def parse(payload: str, metadata: dict, config: dict, message_parser=None) -> Optional[ParseResult]:
    """Clean, optimized main parser with clear operation dispatch."""
    device_id = metadata.get('device_id', 'unknown')
    
    # Input validation
    if not payload:
        logger.warning(f"Empty payload from device {device_id}")
        return None
    
    try:
        # Decode binary data
        binary_data = binascii.unhexlify(payload)
        if binary_data is None:
            logger.error(f"Failed to decode hex payload from {device_id}")
            return None
        
        # Decode packet
        decoded = LightProtocolDecoder.decode_packet(binary_data)
        operation = decoded['operation']
        logger.info(f"Device {device_id} operation: {operation}")
        
        # Operation dispatch table for clean separation
        operation_handlers = {
            'read_task': OperationHandler.handle_read_task,
            'read_all': OperationHandler.handle_read_all,
            'create_task': OperationHandler.handle_create_task,
            'delete_task': OperationHandler.handle_delete_task,
            'delete_all': OperationHandler.handle_delete_all
        }
        
        # Execute operation handler
        handler = operation_handlers.get(operation)
        if handler:
            readings = handler(device_id, decoded, metadata)
            return ParseResult("data", readings)
        
        # Unknown or unhandled operation
        logger.debug(f"Unhandled operation '{operation}' from device {device_id}")
        return None
        
    except struct.error as e:
        logger.error(f"Struct unpacking error for device {device_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing message from {device_id}: {e}")
        return None

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
        return LightProtocolDecoder.encode_packet(packet_data, format="hex")

def format_command(command: dict, config: dict, message_parser=None) -> dict:
    """Optimized command dispatcher with lookup table."""
    command_type = command.get('command_type', '')
    
    # Command handler lookup table for O(1) dispatch
    handlers = {
        'light_config_task': _format_light_config_task_command,
        'light_read_single': _format_read_single_command,
        'light_read_all': _format_read_all_command, 
        'light_delete_task': _format_delete_task_command,
        'light_delete_all': _format_delete_all_command
    }
    
    handler = handlers.get(command_type)
    if not handler:
        raise ValueError(f"Unknown light command type: {command_type}")
        
    payload = handler(command, config)
    metadata = command.get('metadata', {})
    broker_id = metadata.get('broker_parameter')
    command_id = command.get('request_id')
    
    formatted_command = {
        "payload": {"uid": f"{command_id}-{broker_id}", **payload},
        "metadata": {
            "device_id": command['device_id'],
            "broker_id": broker_id,
            "topic": f"broker_data/{broker_id}/command",
            "qos": 0,
            "retain": False
        }
    }
    logger.info(f"Formatted command: {formatted_command}")
    return formatted_command

def _format_light_config_task_command(command: dict, config: dict) -> dict:
    """Format command to create or update a task (get_set=1, all=0, clear=0)."""
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
    
    return LightProtocolDecoder.encode_packet(packet_data)

def _format_read_single_command(command: dict, config: dict) -> dict:
    """Format command to read a single task by ID (get_set=0, all=0, clear=0)."""
    task_id = command['payload'].get('task_id', 0)
    
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'],
        {'task_id': task_id, 'get_set': 0},  # Read single task (get_set=0, all=0, clear=0)
        {'task_id': task_id}
    )
    
    return LightProtocolDecoder.encode_packet(packet_data)

def _format_read_all_command(command: dict, config: dict) -> dict:
    """Format command to read all tasks - returns task count (get_set=0, all=1, clear=0)."""
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'],
        {'all': 1, 'get_set': 0},  # Read all
        {}
    )
    logger.debug(f"Read all packet data: {packet_data}")
    return LightProtocolDecoder.encode_packet(packet_data)

def _format_delete_task_command(command: dict, config: dict) -> dict:
    """Format command to delete a single task by ID (get_set=1, all=0, clear=1)."""
    task_id = command['payload'].get('task_id', 0)
    
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'],
        {'task_id': task_id, 'clear': 1, 'get_set': 1},  # Delete specific
        {'task_id': task_id}
    )
    
    return LightProtocolDecoder.encode_packet(packet_data)

def _format_delete_all_command(command: dict, config: dict) -> dict:
    """Format command to delete all tasks (get_set=1, all=1, clear=1)."""
    packet_data = CommandFormatter._build_base_packet(
        command['device_id'], 
        {'all': 1, 'clear': 1, 'get_set': 1},  # Delete all
        {}
    )
    
    return LightProtocolDecoder.encode_packet(packet_data)