# storage/parser_scripts/light_controller_parser.py
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

class UnifiedReadingManager:
    """Manages a single unified cache reading for light controller devices."""
    
    # Constants for datatype structure
    MAX_TASKS = 15
    
    # Datatype labels matching the database schema
    DATATYPE_LABELS = [
        "task_time_beg", "task_duration", "task_day", "task_channel",
        "task_active", "task_run_once", "lux_rate_c1", "lux_rate_c2",
        "lux_name_c1", "lux_name_c2", "lux_per_month", "task_count"
    ]
    
    DATATYPE_DISPLAY_NAMES = [
        "Tâche - Heure début", "Tâche - Durée (min)", "Tâche - Jour",
        "Tâche - Canal", "Tâche - Actif", "Tâche - Unique",
        "Canal 1 - Taux lux/h", "Canal 2 - Taux lux/h", "Canal 1 - Nom",
        "Canal 2 - Nom", "Quota mensuel lux", "Nombre de tâches"
    ]
    
    # Value indices in unified reading array
    VALUE_INDICES = {
        "task_time_beg": 0,    # List: [task0_time, task1_time, ..., task14_time]
        "task_duration": 1,    # List: [task0_duration, task1_duration, ...]
        "task_day": 2,         # List: [task0_day, task1_day, ...]
        "task_channel": 3,     # List: [task0_channel, task1_channel, ...]
        "task_active": 4,      # List: [task0_active, task1_active, ...]
        "task_run_once": 5,    # List: [task0_run_once, task1_run_once, ...]
        "lux_rate_c1": 6,      # Single integer: Channel 1 lux rate
        "lux_rate_c2": 7,      # Single integer: Channel 2 lux rate
        "lux_name_c1": 8,      # Single string: Channel 1 name
        "lux_name_c2": 9,      # Single string: Channel 2 name
        "lux_per_month": 10,   # Single integer: Monthly lux quota
        "task_count": 11       # Single integer: Total number of active tasks
    }
    
    @staticmethod
    def create_base_reading(device_id: str, metadata: dict) -> dict:
        """Create initial unified reading with all datatype labels."""
        # Initialize task-related values as lists of 15 elements
        values = [
            [0] * UnifiedReadingManager.MAX_TASKS,        # task_time_beg
            [0] * UnifiedReadingManager.MAX_TASKS,        # task_duration
            [0] * UnifiedReadingManager.MAX_TASKS,        # task_day
            [0] * UnifiedReadingManager.MAX_TASKS,        # task_channel
            [False] * UnifiedReadingManager.MAX_TASKS,    # task_active
            [False] * UnifiedReadingManager.MAX_TASKS,    # task_run_once
            0,      # lux_rate_c1
            0,      # lux_rate_c2
            "",     # lux_name_c1
            "",     # lux_name_c2
            0,      # lux_per_month
            0       # task_count
        ]
        
        reading = SensorReading(
            device_id=device_id,
            values=tuple(values),
            labels=tuple(UnifiedReadingManager.DATATYPE_LABELS),
            display_names=tuple(UnifiedReadingManager.DATATYPE_DISPLAY_NAMES),
            timestamp=datetime.now(),
            metadata={
                'persist': [False] * len(UnifiedReadingManager.DATATYPE_LABELS),
                'operation': 'initialization',
                'task_count': 0,
                **metadata
            },
            index='L'
        )
        
        return asdict(reading)
    
    @staticmethod
    def get_or_create_unified_reading(metadata: dict, device_id: str) -> dict:
        """Get existing reading from metadata.readings or create new one."""
        if 'readings' not in metadata:
            metadata['readings'] = {}
        
        if 'L' not in metadata['readings']:
            metadata['readings']['L'] = UnifiedReadingManager.create_base_reading(device_id, metadata)
        
        return metadata['readings']['L']
    
    @staticmethod
    def update_reading_from_operation(base_reading: dict, decoded: dict, operation_type: str, metadata: dict) -> dict:
        """Update unified reading based on operation type and data."""
        # Convert values to mutable lists for updating
        values = [list(v) if isinstance(v, (list, tuple)) else v for v in base_reading['values']]
        
        if operation_type == 'read_all':
            # Update task count from decoded alarm.task_id
            task_count = decoded['alarm']['task_id']
            values[UnifiedReadingManager.VALUE_INDICES['task_count']] = task_count
            logger.info(f"Updated task_count to {task_count}")
            
        elif operation_type in ['create_task', 'read_task']:
            # Update specific task data at task_id index
            alarm = decoded['alarm']
            task_id = alarm['task_id']
            
            if 0 <= task_id < UnifiedReadingManager.MAX_TASKS:
                # Update task lists at specific task_id index
                values[UnifiedReadingManager.VALUE_INDICES['task_time_beg']][task_id] = alarm['time_beg']
                values[UnifiedReadingManager.VALUE_INDICES['task_duration']][task_id] = alarm['duration']
                values[UnifiedReadingManager.VALUE_INDICES['task_day']][task_id] = alarm['day']
                values[UnifiedReadingManager.VALUE_INDICES['task_channel']][task_id] = alarm['channel']
                values[UnifiedReadingManager.VALUE_INDICES['task_active']][task_id] = bool(alarm['active'])
                values[UnifiedReadingManager.VALUE_INDICES['task_run_once']][task_id] = bool(alarm['run_once'])
                
                # Update task count if this is a new active task
                if operation_type == 'create_task' and alarm['active']:
                    current_task_count = values[UnifiedReadingManager.VALUE_INDICES['task_count']]
                    # Count active tasks
                    active_count = sum(1 for active in values[UnifiedReadingManager.VALUE_INDICES['task_active']] if active)
                    values[UnifiedReadingManager.VALUE_INDICES['task_count']] = active_count
                
                logger.info(f"Updated task {task_id} data for operation {operation_type}")
            else:
                logger.warning(f"Invalid task_id {task_id}, must be 0-{UnifiedReadingManager.MAX_TASKS-1}")
                
        elif operation_type == 'delete_task':
            # Reset specific task to default values
            task_id = decoded['alarm']['task_id']
            
            if 0 <= task_id < UnifiedReadingManager.MAX_TASKS:
                values[UnifiedReadingManager.VALUE_INDICES['task_time_beg']][task_id] = 0
                values[UnifiedReadingManager.VALUE_INDICES['task_duration']][task_id] = 0
                values[UnifiedReadingManager.VALUE_INDICES['task_day']][task_id] = 0
                values[UnifiedReadingManager.VALUE_INDICES['task_channel']][task_id] = 0
                values[UnifiedReadingManager.VALUE_INDICES['task_active']][task_id] = False
                values[UnifiedReadingManager.VALUE_INDICES['task_run_once']][task_id] = False
                
                # Update task count
                active_count = sum(1 for active in values[UnifiedReadingManager.VALUE_INDICES['task_active']] if active)
                values[UnifiedReadingManager.VALUE_INDICES['task_count']] = active_count
                
                logger.info(f"Deleted task {task_id}, active count now {active_count}")
            else:
                logger.warning(f"Invalid task_id {task_id}, must be 0-{UnifiedReadingManager.MAX_TASKS-1}")
                
        elif operation_type == 'delete_all':
            # Reset all task lists to default values
            values[UnifiedReadingManager.VALUE_INDICES['task_time_beg']] = [0] * UnifiedReadingManager.MAX_TASKS
            values[UnifiedReadingManager.VALUE_INDICES['task_duration']] = [0] * UnifiedReadingManager.MAX_TASKS
            values[UnifiedReadingManager.VALUE_INDICES['task_day']] = [0] * UnifiedReadingManager.MAX_TASKS
            values[UnifiedReadingManager.VALUE_INDICES['task_channel']] = [0] * UnifiedReadingManager.MAX_TASKS
            values[UnifiedReadingManager.VALUE_INDICES['task_active']] = [False] * UnifiedReadingManager.MAX_TASKS
            values[UnifiedReadingManager.VALUE_INDICES['task_run_once']] = [False] * UnifiedReadingManager.MAX_TASKS
            values[UnifiedReadingManager.VALUE_INDICES['task_count']] = 0
            
            logger.info("Deleted all tasks")
        
        # Update the reading with new values and metadata
        updated_reading = base_reading.copy()
        updated_reading['values'] = tuple(values)
        updated_reading['timestamp'] = datetime.now().isoformat()
        updated_reading['metadata'].update({
            'operation': operation_type,
            'task_count': values[UnifiedReadingManager.VALUE_INDICES['task_count']],
            **metadata
        })
        
        return updated_reading


class OperationHandler:
    """Handles different operation types using unified reading approach."""
    
    @staticmethod
    def handle_read_all(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Read All response using unified reading."""
        # Get or create unified reading from metadata
        unified_reading = UnifiedReadingManager.get_or_create_unified_reading(metadata, device_id)
        
        # Update reading with read_all operation data
        updated_reading = UnifiedReadingManager.update_reading_from_operation(
            unified_reading, decoded, 'read_all', metadata
        )
        
        # Store updated reading back in metadata
        metadata['readings']['L'] = updated_reading
        logger.info(updated_reading)
        return [updated_reading]
    
    @staticmethod
    def handle_create_task(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle task creation/update using unified reading."""
        # Get or create unified reading from metadata
        unified_reading = UnifiedReadingManager.get_or_create_unified_reading(metadata, device_id)
        
        # Update reading with create_task operation data
        updated_reading = UnifiedReadingManager.update_reading_from_operation(
            unified_reading, decoded, 'create_task', metadata
        )
        
        # Store updated reading back in metadata
        metadata['readings']['L'] = updated_reading
        
        return [updated_reading]
    
    @staticmethod
    def handle_read_task(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Read Single Task operation using unified reading."""
        # Get or create unified reading from metadata
        unified_reading = UnifiedReadingManager.get_or_create_unified_reading(metadata, device_id)
        
        # Update reading with read_task operation data
        updated_reading = UnifiedReadingManager.update_reading_from_operation(
            unified_reading, decoded, 'read_task', metadata
        )
        
        # Store updated reading back in metadata
        metadata['readings']['L'] = updated_reading
        
        return [updated_reading]
    
    @staticmethod
    def handle_delete_task(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Delete Task operation using unified reading."""
        # Get or create unified reading from metadata
        unified_reading = UnifiedReadingManager.get_or_create_unified_reading(metadata, device_id)
        
        # Update reading with delete_task operation data
        updated_reading = UnifiedReadingManager.update_reading_from_operation(
            unified_reading, decoded, 'delete_task', metadata
        )
        
        # Store updated reading back in metadata
        metadata['readings']['L'] = updated_reading
        
        return [updated_reading]
    
    @staticmethod
    def handle_delete_all(device_id: str, decoded: Dict[str, Any], metadata: dict) -> List[dict]:
        """Handle Delete All Tasks operation using unified reading."""
        # Get or create unified reading from metadata
        unified_reading = UnifiedReadingManager.get_or_create_unified_reading(metadata, device_id)
        
        # Update reading with delete_all operation data
        updated_reading = UnifiedReadingManager.update_reading_from_operation(
            unified_reading, decoded, 'delete_all', metadata
        )
        
        # Store updated reading back in metadata
        metadata['readings']['L'] = updated_reading
        
        return [updated_reading]

def parse(payload: str, metadata: dict, config: dict, message_parser=None) -> Optional[ParseResult]:
    """
    Main parser using unified reading system for light controller devices.
    
    Returns a single unified reading that matches the datatype schema exactly,
    with task-related values stored as lists where index corresponds to task_id.
    The reading is persisted in metadata.readings['L'] for state management.
    
    Args:
        payload: Hex-encoded binary response from light controller
        metadata: Message metadata (will be updated with readings)
        config: Hardware configuration
        message_parser: Optional message parser (unused)
        
    Returns:
        ParseResult with single unified reading or None on error
    """
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