from abc import ABC, abstractmethod
from shared.models.translation import RawData, TranslationResult

class BaseTranslator(ABC):
    """Base class for all payload translators."""
    
    @abstractmethod
    def can_handle(self, raw_data: RawData) -> bool:
        """Check if this translator can handle the raw data."""
        pass
    
    @abstractmethod
    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """Extract device ID from raw data."""
        pass 