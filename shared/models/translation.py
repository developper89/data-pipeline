from typing import Optional, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from shared.translation.base import BaseTranslator

@dataclass
class RawData:
    """Raw data input for translation layer."""
    payload_bytes: bytes
    protocol: str
    metadata: Dict[str, Any] = None
    path: Optional[str] = None  # For any protocol path (MQTT topics, CoAP paths, HTTP URLs, etc.)
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

@dataclass  
class TranslationResult:
    """Result of device ID extraction/translation operation."""
    success: bool
    device_id: Optional[str] = None
    translator_used: Optional[str] = None
    translator_type: Optional[str] = None
    translator: Optional['BaseTranslator'] = None  # The actual translator instance
    metadata: Dict[str, Any] = None
    error: Optional[str] = None
    raw_data: Optional['RawData'] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {} 