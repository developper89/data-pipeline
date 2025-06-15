from typing import Optional, Tuple, Any, Dict
import logging
from .proto_compiler import ProtobufCompiler, check_protoc_available

logger = logging.getLogger(__name__)

class ProtobufMessageParser:
    """Generic protobuf message parser that works with any manufacturer's schemas."""

    def __init__(self, manufacturer: str, message_types: Dict[str, Any]):
        self.manufacturer = manufacturer
        self.message_types = message_types
        self.compiler = None
        self._load_proto_modules()

    def _load_proto_modules(self):
        """Load compiled protobuf modules for this manufacturer."""
        self.proto_modules = {}
        
        # Check if protobuf is available
        try:
            import google.protobuf
        except ImportError:
            logger.error("protobuf package not installed. Run: pip install protobuf>=4.21.0")
            return

        # Check if protoc compiler is available
        if not check_protoc_available():
            logger.error("protoc compiler not available. Please install protobuf compiler.")
            return

        # Initialize dynamic compiler
        self.compiler = ProtobufCompiler(self.manufacturer)
        
        # Compile .proto files to Python modules
        compiled_modules = self.compiler.compile_proto_files()
        
        if not compiled_modules:
            logger.warning(f"No protobuf modules compiled for {self.manufacturer}")
            return

        # Map message types to compiled classes
        for message_type, config in self.message_types.items():
            try:
                proto_module = config.get('proto_module')
                proto_class = config.get('proto_class')

                if proto_module and proto_class:
                    cls = self.compiler.get_proto_class(proto_module, proto_class)
                    
                    if cls:
                        self.proto_modules[message_type] = {
                            'class': cls,
                            'required_fields': config.get('required_fields', [])
                        }
                        logger.debug(f"Loaded {message_type} class: {proto_class}")
                    else:
                        logger.warning(f"Failed to get class {proto_class} from module {proto_module}")
                        
            except Exception as e:
                logger.warning(f"Failed to load {message_type} for {self.manufacturer}: {e}")
                continue

        logger.info(f"Successfully loaded {len(self.proto_modules)} protobuf message types for {self.manufacturer}")

    def detect_message_type(self, payload: bytes) -> Optional[str]:
        """Detect the type of protobuf message."""
        logger.debug(f"Detecting message type for payload of {len(payload)} bytes")
        
        for message_type, proto_info in self.proto_modules.items():
            logger.debug(f"Trying to parse as {message_type}...")
            if self._try_parse_message(payload, proto_info, message_type):
                logger.info(f"Successfully detected message type: {message_type}")
                return message_type
            else:
                logger.debug(f"Failed to parse as {message_type}")
                
        logger.warning(f"Could not detect message type for payload. Available types: {list(self.proto_modules.keys())}")
        return None

    def parse_message(self, payload: bytes) -> Tuple[str, Any]:
        """Parse protobuf message and return type and parsed object."""
        logger.debug(f"Parsing protobuf message of {len(payload)} bytes")
        
        message_type = self.detect_message_type(payload)

        if message_type and message_type in self.proto_modules:
            proto_info = self.proto_modules[message_type]
            message = proto_info['class']()
            message.ParseFromString(payload)
            logger.info(f"Successfully parsed {message_type} message")
            
            # Log available fields for debugging
            if hasattr(message, 'DESCRIPTOR'):
                available_fields = [field.name for field in message.DESCRIPTOR.fields]
                logger.debug(f"Parsed {message_type} has fields: {available_fields}")
            
            return message_type, message
        else:
            available_types = list(self.proto_modules.keys())
            logger.error(f"Unknown {self.manufacturer} message type. Available types: {available_types}")
            raise ValueError(f"Unknown {self.manufacturer} message type")
    
    def parse_message_as_type(self, payload: bytes, target_message_type: str) -> Tuple[Optional[str], Any]:
        """
        Try to parse protobuf message as a specific message type.
        
        Args:
            payload: Raw protobuf bytes
            target_message_type: Specific message type to try parsing as
            
        Returns:
            Tuple of (message_type, parsed_message) if successful, (None, None) if failed
        """
        logger.debug(f"Trying to parse payload as specific message type: {target_message_type}")
        
        if target_message_type not in self.proto_modules:
            available_types = list(self.proto_modules.keys())
            logger.warning(f"Target message type '{target_message_type}' not available. Available types: {available_types}")
            return None, None
        
        proto_info = self.proto_modules[target_message_type]
        
        if self._try_parse_message(payload, proto_info, target_message_type):
            # Successfully parsed - create and return the parsed message
            message = proto_info['class']()
            message.ParseFromString(payload)
            logger.info(f"Successfully parsed as {target_message_type} message")
            
            # Log available fields for debugging
            if hasattr(message, 'DESCRIPTOR'):
                available_fields = [field.name for field in message.DESCRIPTOR.fields]
                logger.debug(f"Parsed {target_message_type} has fields: {available_fields}")
            
            return target_message_type, message
        else:
            logger.debug(f"Failed to parse payload as {target_message_type}")
            return None, None

    def _try_parse_message(self, payload: bytes, proto_info: Dict, message_type: str = "") -> bool:
        """Try to parse payload as specific message type."""
        try:
            message = proto_info['class']()
            message.ParseFromString(payload)

            # Validate required fields exist
            required_fields = proto_info.get('required_fields', [])
            if required_fields:
                logger.debug(f"Checking required fields for {message_type}: {required_fields}")
                
            for field in required_fields:
                if not hasattr(message, field):
                    logger.debug(f"Required field '{field}' not found in {message_type}")
                    return False
                
                # Check if field has a value (protobuf fields can exist but be empty)
                field_value = getattr(message, field)
                if field_value is None or (isinstance(field_value, (str, bytes)) and not field_value):
                    logger.debug(f"Required field '{field}' is empty in {message_type}")
                    return False
                    
                logger.debug(f"Required field '{field}' found with value in {message_type}")

            logger.debug(f"Successfully parsed and validated {message_type}")
            return True
            
        except Exception as e:
            logger.debug(f"Failed to parse as {message_type}: {e}")
            return False
    
    def cleanup(self):
        """Clean up compiler resources."""
        if self.compiler:
            self.compiler.cleanup()
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.cleanup() 