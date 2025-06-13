# shared/translation/protobuf/device_id_extractor.py
import re
from typing import Optional, Any, Dict, List

class ProtobufDeviceIdExtractor:
    """Generic device ID extractor that works with configured field mappings."""

    def __init__(self, extraction_config: Dict[str, Any]):
        self.sources = extraction_config.get('sources', [])
        self.validation = extraction_config.get('validation', {})
        self.last_source_used = None

        # Sort sources by priority
        self.sources = sorted(self.sources, key=lambda x: x.get('priority', 999))

    def extract(self, message_type: str, parsed_message: Any) -> Optional[str]:
        """Extract device ID from parsed message using configured sources."""

        # Try configured sources in priority order
        for source in self.sources:
            if source.get('message_type') != message_type:
                continue

            field_path = source.get('field_path')
            device_id = self._extract_from_field_path(parsed_message, field_path)

            if device_id and self._validate_device_id(device_id):
                self.last_source_used = f"{message_type}.{field_path}"
                return self._normalize_device_id(device_id)

        return None

    def _extract_from_field_path(self, message: Any, field_path: str) -> Optional[str]:
        """Extract value from nested field path (e.g., 'device.info.id')."""
        try:
            current = message
            for field in field_path.split('.'):
                if hasattr(current, field):
                    current = getattr(current, field)
                else:
                    return None

            # Convert to string
            if isinstance(current, (str, bytes)):
                return current.decode('utf-8') if isinstance(current, bytes) else current
            return str(current)

        except Exception:
            return None

    def _validate_device_id(self, device_id: str) -> bool:
        """Validate device ID using configured regex."""
        if not device_id or len(device_id) < 3:
            return False

        regex_pattern = self.validation.get('regex')
        if regex_pattern:
            return bool(re.match(regex_pattern, device_id))

        # Default validation for common formats
        return bool(re.match(r'^[a-zA-Z0-9:_-]{3,32}$', device_id))

    def _normalize_device_id(self, device_id: str) -> str:
        """Normalize device ID using configured rules."""
        # Remove configured prefix
        prefix_to_remove = self.validation.get('remove_prefix')
        if prefix_to_remove and device_id.startswith(prefix_to_remove):
            device_id = device_id[len(prefix_to_remove):]

        # Apply normalization
        if self.validation.get('normalize', True):
            device_id = device_id.lower()

        return device_id 