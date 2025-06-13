from typing import Optional, Dict, Any
import logging
from shared.translation.base import BaseTranslator
from shared.models.translation import RawData, TranslationResult
from .message_parser import ProtobufMessageParser
from .device_id_extractor import ProtobufDeviceIdExtractor

logger = logging.getLogger(__name__)

class ProtobufTranslator(BaseTranslator):
    """Generic protobuf translator that works with any manufacturer's schemas."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.manufacturer = config.get('manufacturer', 'unknown')

        # Initialize components with manufacturer-specific config
        self.message_parser = ProtobufMessageParser(
            manufacturer=self.manufacturer,
            message_types=config.get('message_types', {})
        )

        self.device_id_extractor = ProtobufDeviceIdExtractor(
            config.get('device_id_extraction', {})
        )

    def can_handle(self, raw_data: RawData) -> bool:
        """Check if this translator can handle the protobuf data."""
        # Quick protobuf format detection
        if not self._is_likely_protobuf(raw_data.payload_bytes):
            return False

        # Try to parse with manufacturer's schemas
        try:
            message_type = self.message_parser.detect_message_type(raw_data.payload_bytes)
            return message_type is not None
        except Exception:
            return False

    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """Extract device ID from protobuf payload."""
        try:
            # Parse the protobuf message
            message_type, parsed_message = self.message_parser.parse_message(
                raw_data.payload_bytes
            )

            # Extract device ID using configured field mappings
            device_id = self.device_id_extractor.extract(message_type, parsed_message)

            if device_id:
                return TranslationResult(
                    device_id=device_id,
                    success=True,
                    translator_used=f"protobuf_{self.manufacturer}",
                    metadata={
                        "manufacturer": self.manufacturer,
                        "message_type": message_type,
                        "extraction_source": self.device_id_extractor.last_source_used
                    }
                )
            else:
                return TranslationResult(
                    success=False,
                    error=f"No device ID found in {self.manufacturer} protobuf message"
                )

        except Exception as e:
            logger.exception(f"Protobuf parsing failed for {self.manufacturer}: {e}")
            return TranslationResult(
                success=False,
                error=f"Protobuf parsing error: {str(e)}"
            )

    def _is_likely_protobuf(self, payload: bytes) -> bool:
        """Quick check if payload looks like protobuf."""
        if len(payload) < 2:
            return False
        # Protobuf typically starts with field tags
        return payload[0] in [0x08, 0x0a, 0x10, 0x12, 0x18, 0x1a, 0x20, 0x22] 