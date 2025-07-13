# shared/translation/factory.py
import logging
from typing import Dict, Any, Optional
from .base import BaseTranslator
from .protobuf.translator import ProtobufTranslator
from .proprietary.translator import ProprietaryTranslator

logger = logging.getLogger(__name__)

class TranslatorFactory:
    """Factory for creating translator instances based on configuration."""
    
    @staticmethod
    def create_translator(translator_config: Dict[str, Any]) -> Optional[BaseTranslator]:
        """
        Create a translator based on the provided configuration.
        
        Args:
            translator_config: Configuration dictionary containing type and config
            
        Returns:
            Translator instance or None if type is not supported
        """
        translator_type = translator_config.get('type', '').lower()
        config = translator_config.get('config', {})
        
        logger.debug(f"Creating translator of type: {translator_type}")
        
        if translator_type == 'protobuf':
            return ProtobufTranslator(config)
        elif translator_type == 'proprietary':
            return ProprietaryTranslator(config)
        else:
            logger.error(f"Unknown translator type: {translator_type}")
            return None

 