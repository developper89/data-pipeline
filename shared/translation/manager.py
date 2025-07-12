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
                logger.debug(f"Using translator: {translator.__class__.__name__}")
                result = translator.extract_device_id(raw_data)  # Use translate method instead
                
                # If successful, return the result with translator instance
                if result.success and result.device_id:
                    logger.debug(f"Successfully extracted device ID '{result.device_id}' using {translator.__class__.__name__}")
                    # Add the translator instance to the result
                    result.translator = translator
                    return result
                else:
                    logger.debug(f"Translator {translator.__class__.__name__} failed or returned no device ID")
                    continue
                    
            except Exception as e:
                logger.warning(f"Translator {translator.__class__.__name__} failed: {e}")
                continue
        
        return TranslationResult(
            success=False,
            error="No translator could handle this data",
            raw_data=raw_data
        )
    
    def add_translator(self, translator: BaseTranslator):
        """Add a new translator to the manager."""
        self.translators.append(translator)
    
    def get_translator_count(self) -> int:
        """Get the number of registered translators."""
        return len(self.translators) 