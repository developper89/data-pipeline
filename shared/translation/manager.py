from typing import List, Optional
import logging
from shared.models.translation import RawData, TranslationResult
from .base import BaseTranslator

logger = logging.getLogger(__name__)

class TranslationManager:
    """Manages multiple translators for device ID extraction."""
    
    def __init__(self, translators: List[BaseTranslator]):
        self.translators = translators
        
    def extract_device_id(self, raw_data: RawData) -> TranslationResult:
        """
        Extract device ID using the first translator that can handle the data.
        
        Args:
            raw_data: RawData object containing payload bytes and protocol info
            
        Returns:
            TranslationResult with device ID or error information
        """
        for translator in self.translators:
            try:
                if translator.can_handle(raw_data):
                    logger.debug(f"Using translator: {translator.__class__.__name__}")
                    return translator.extract_device_id(raw_data)
            except Exception as e:
                logger.warning(f"Translator {translator.__class__.__name__} failed: {e}")
                continue
        
        return TranslationResult(
            success=False,
            error="No translator could handle this data"
        )
    
    def add_translator(self, translator: BaseTranslator):
        """Add a new translator to the manager."""
        self.translators.append(translator)
    
    def get_translator_count(self) -> int:
        """Get the number of registered translators."""
        return len(self.translators) 