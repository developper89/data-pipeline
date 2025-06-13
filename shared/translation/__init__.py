# shared/translation/__init__.py

"""
Translation layer for handling manufacturer-specific payload formats.

This module provides a pluggable architecture for extracting device IDs from
different payload formats (protobuf, custom binary, etc.) across various
manufacturers and protocols.
"""

from .base import BaseTranslator
from .manager import TranslationManager
from .protobuf.translator import ProtobufTranslator

__all__ = [
    'BaseTranslator',
    'TranslationManager', 
    'ProtobufTranslator'
] 