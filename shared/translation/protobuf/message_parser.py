from typing import Optional, Tuple, Any, Dict
import logging
from .proto_loader import ProtoModuleLoader

logger = logging.getLogger(__name__)

class ProtobufMessageParser:
    """Generic protobuf message parser that works with any manufacturer's schemas."""

    def __init__(self, manufacturer: str, message_types: Dict[str, Any]):
        self.manufacturer = manufacturer
        self.message_types = message_types
        self._load_proto_modules()

    def _load_proto_modules(self):
        """Load compiled protobuf modules for this manufacturer."""
        self.proto_modules = {}

        for message_type, config in self.message_types.items():
            try:
                proto_module = config.get('proto_module')
                proto_class = config.get('proto_class')

                if proto_module and proto_class:
                    cls = ProtoModuleLoader.get_proto_class(
                        self.manufacturer, proto_module, proto_class
                    )

                    self.proto_modules[message_type] = {
                        'class': cls,
                        'required_fields': config.get('required_fields', [])
                    }
            except ImportError as e:
                logger.warning(f"Failed to load {message_type} for {self.manufacturer}: {e}")
                continue

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