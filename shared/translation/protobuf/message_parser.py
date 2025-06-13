from typing import Optional, Tuple, Any, Dict
import logging
from .proto_compiler import ProtobufCompiler, check_protoc_available

logging.basicConfig(
    format= '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
)

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
        for message_type, proto_info in self.proto_modules.items():
            if self._try_parse_message(payload, proto_info):
                return message_type
        return None

    def parse_message(self, payload: bytes) -> Tuple[str, Any]:
        """Parse protobuf message and return type and parsed object."""
        message_type = self.detect_message_type(payload)

        if message_type and message_type in self.proto_modules:
            proto_info = self.proto_modules[message_type]
            message = proto_info['class']()
            message.ParseFromString(payload)
            return message_type, message
        else:
            raise ValueError(f"Unknown {self.manufacturer} message type")

    def _try_parse_message(self, payload: bytes, proto_info: Dict) -> bool:
        """Try to parse payload as specific message type."""
        try:
            message = proto_info['class']()
            message.ParseFromString(payload)

            # Validate required fields exist
            required_fields = proto_info.get('required_fields', [])
            for field in required_fields:
                if not hasattr(message, field):
                    return False

            return True
        except Exception:
            return False
    
    def cleanup(self):
        """Clean up compiler resources."""
        if self.compiler:
            self.compiler.cleanup()
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.cleanup() 