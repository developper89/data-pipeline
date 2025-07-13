# shared/translation/pattern/__init__.py

"""
Protocol-agnostic pattern-based translation module.

This module provides pattern-based device ID extraction that works across
different communication protocols (MQTT, CoAP, HTTP, etc.).
"""

from .translator import ProprietaryTranslator

__all__ = [
    'ProprietaryTranslator'
] 