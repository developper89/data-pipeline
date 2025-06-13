# Normalizer-Translation Layer Integration Plan

**Date**: December 2024  
**Version**: 1.0  
**Purpose**: Integration plan for using the translation layer in the normalizer for protobuf payload parsing

## 1. Overview

The normalizer currently uses parser scripts to decode `payload_hex` from `RawMessage`. For protobuf payloads (like Efento devices), we need to integrate the translation layer to handle the decoding before the standard parser scripts run.

## 2. Current Flow vs. New Flow

### 2.1 Current Flow

```
CoAP Connector → RawMessage → Normalizer → Parser Script → StandardizedOutput
```

### 2.2 New Flow with Translation Layer

```
CoAP Connector → RawMessage + translator field → Normalizer → Translation Layer → Parser Script → StandardizedOutput
```

## 3. RawMessage Schema Enhancement

### 3.1 Add Translator Field

```python
# shared/models/common.py

class RawMessage(BaseMessage):
    device_id: str
    payload_hex: str  # Hex-encoded binary data
    protocol: str     # e.g., 'mqtt', 'coap'
    metadata: dict = {}
    translator: Optional[str] = None  # NEW FIELD: e.g., 'protobuf_efento', 'protobuf_manufacturer_x'
```

### 3.2 Connector Updates

Update connectors to specify translator when known:

```python
# connectors/coap_connector/resources.py

# For Efento devices (detected by device_id pattern or configuration)
if self._is_efento_device(device_id):
    raw_message = RawMessage(
        device_id=device_id,
        payload_hex=payload_hex,
        protocol="coap",
        translator="protobuf_efento",  # Specify translator
        metadata=coap_metadata
    )
else:
    # Standard message without translator
    raw_message = RawMessage(
        device_id=device_id,
        payload_hex=payload_hex,
        protocol="coap",
        metadata=coap_metadata
    )
```

## 4. Normalizer Integration

### 4.1 Enhanced Processing Flow

```python
# normalizer/service.py

async def _process_message(self, raw_msg_record) -> bool:
    # ... existing validation code ...

    # 2. Check if translation is needed
    if raw_message.translator:
        # Use translation layer to parse protobuf
        translated_data = await self._translate_payload(raw_message)
        if translated_data:
            # Use translated data instead of raw payload
            processed_data = await self._process_translated_data(raw_message, translated_data)
        else:
            # Translation failed, handle error
            await self._publish_processing_error("Translation failed", ...)
            return True
    else:
        # Standard processing with parser scripts
        processed_data = await self._process_with_parser_script(raw_message)

    # ... rest of processing ...
```

### 4.2 Translation Layer Integration

```python
# normalizer/service.py

from shared.translation.manager import TranslationManager
from shared.translation.protobuf.translator import ProtobufTranslator

class NormalizerService:
    def __init__(self, ...):
        # ... existing initialization ...

        # Initialize translation manager
        self.translation_manager = self._initialize_translation_manager()

    def _initialize_translation_manager(self):
        """Initialize translation manager with available translators."""
        translators = []

        # Load Efento protobuf translator
        efento_config = {
            "type": "protobuf",
            "manufacturer": "efento",
            "device_id_extraction": {
                "sources": [
                    {"message_type": "measurements", "field_path": "serialNum", "priority": 1}
                ],
                "validation": {"regex": "^[a-f0-9]{16}$", "normalize": False},
                "decoding": "base64_hex"  # base64 decode then hex encode
            },
            "message_types": {
                "measurements": {
                    "proto_class": "ProtoMeasurements",
                    "proto_module": "proto_measurements_pb2",
                    "required_fields": ["serialNum", "batteryStatus"]
                },
                "device_info": {
                    "proto_class": "ProtoDeviceInfo",
                    "proto_module": "proto_device_info_pb2",
                    "required_fields": ["serialNumber"]
                }
            }
        }

        translators.append(ProtobufTranslator(efento_config))
        return TranslationManager(translators)

    async def _translate_payload(self, raw_message: RawMessage) -> Optional[dict]:
        """Translate protobuf payload to structured data."""
        try:
            # Convert hex payload back to bytes
            payload_bytes = bytes.fromhex(raw_message.payload_hex)

            # Find appropriate translator
            translator = self.translation_manager.get_translator(raw_message.translator)
            if not translator:
                logger.error(f"No translator found for: {raw_message.translator}")
                return None

            # Parse protobuf payload
            result = translator.parse_payload(payload_bytes)
            if result.success:
                return result.parsed_data
            else:
                logger.error(f"Translation failed: {result.error}")
                return None

        except Exception as e:
            logger.exception(f"Translation error: {e}")
            return None
```

## 5. Translation Manager Implementation

### 5.1 Manager Class

```python
# shared/translation/manager.py

from typing import List, Optional, Dict, Any
from .base import BaseTranslator

class TranslationManager:
    """Manages multiple translators for different manufacturers/protocols."""

    def __init__(self, translators: List[BaseTranslator]):
        self.translators = {t.get_name(): t for t in translators}

    def get_translator(self, translator_name: str) -> Optional[BaseTranslator]:
        """Get translator by name."""
        return self.translators.get(translator_name)

    def add_translator(self, translator: BaseTranslator):
        """Add a new translator."""
        self.translators[translator.get_name()] = translator

    def list_translators(self) -> List[str]:
        """List available translator names."""
        return list(self.translators.keys())
```

### 5.2 Enhanced Protobuf Translator

```python
# shared/translation/protobuf/translator.py

from typing import Dict, Any, Optional
import logging
from google.protobuf.json_format import MessageToDict
import base64
from .proto_loader import ProtoModuleLoader
from .message_parser import ProtobufMessageParser

logger = logging.getLogger(__name__)

class ProtobufTranslator(BaseTranslator):
    """Enhanced protobuf translator with payload parsing capabilities."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.manufacturer = config.get('manufacturer', 'unknown')
        self.message_parser = ProtobufMessageParser(
            manufacturer=self.manufacturer,
            message_types=config.get('message_types', {})
        )

    def get_name(self) -> str:
        """Get translator name."""
        return f"protobuf_{self.manufacturer}"

    def parse_payload(self, payload_bytes: bytes) -> 'ParseResult':
        """Parse protobuf payload and return structured data."""
        try:
            # Detect message type and parse
            message_type, parsed_message = self.message_parser.parse_message(payload_bytes)

            # Convert to dictionary for easier processing
            parsed_dict = MessageToDict(parsed_message, preserving_proto_field_name=True)

            # Extract device ID
            device_id = self._extract_device_id(message_type, parsed_dict)

            # Structure the result for normalizer
            result_data = {
                'message_type': message_type,
                'device_id': device_id,
                'manufacturer': self.manufacturer,
                'parsed_message': parsed_dict,
                'raw_size': len(payload_bytes)
            }

            return ParseResult(success=True, parsed_data=result_data)

        except Exception as e:
            logger.exception(f"Protobuf parsing failed for {self.manufacturer}: {e}")
            return ParseResult(success=False, error=str(e))

    def _extract_device_id(self, message_type: str, parsed_dict: dict) -> Optional[str]:
        """Extract device ID from parsed message."""
        extraction_config = self.config.get('device_id_extraction', {})

        for source in extraction_config.get('sources', []):
            if source.get('message_type') == message_type:
                field_path = source.get('field_path')
                raw_value = self._get_nested_value(parsed_dict, field_path)

                if raw_value:
                    # Handle decoding if specified
                    decoding = extraction_config.get('decoding')
                    if decoding == 'base64_hex':
                        # Efento style: base64 decode then hex encode
                        decoded_bytes = base64.b64decode(raw_value)
                        return decoded_bytes.hex()
                    else:
                        return str(raw_value)

        return None

    def _get_nested_value(self, data: dict, field_path: str):
        """Get value from nested dictionary using dot notation."""
        try:
            current = data
            for field in field_path.split('.'):
                current = current[field]
            return current
        except (KeyError, TypeError):
            return None

class ParseResult:
    """Result of payload parsing."""

    def __init__(self, success: bool, parsed_data: dict = None, error: str = None):
        self.success = success
        self.parsed_data = parsed_data or {}
        self.error = error
```

## 6. Efento-Specific Parser Implementation

### 6.1 Efento Parser Script

```python
# storage/parser_scripts/efento_measurements_parser.py

"""
Efento protobuf measurements parser.
This parser works with pre-translated protobuf data from the translation layer.
"""

import json
import math
from datetime import datetime
from typing import Dict, Any, List

def parse(payload: str, metadata: dict = None) -> Dict[str, Any]:
    """
    Parse Efento protobuf data that was already translated by the translation layer.

    Args:
        payload: JSON string containing translated protobuf data
        metadata: Additional metadata from the raw message

    Returns:
        Parsed data in standard format
    """
    try:
        # Parse the translated data
        translated_data = json.loads(payload)
        parsed_message = translated_data['parsed_message']

        # Extract basic information
        device_id = translated_data['device_id']
        message_type = translated_data['message_type']

        result = {
            'device_id': device_id,
            'message_type': message_type,
            'manufacturer': 'efento',
            'battery_status': parsed_message.get('batteryStatus', 'unknown'),
            'channels': [],
            'measurement_period': _calculate_measurement_period(parsed_message),
            'parsed_at': datetime.utcnow().isoformat()
        }

        # Process channels based on message type
        if message_type == 'measurements':
            result['channels'] = _parse_measurement_channels(parsed_message)
        elif message_type == 'device_info':
            result['device_info'] = _parse_device_info(parsed_message)

        return result

    except Exception as e:
        raise ValueError(f"Failed to parse Efento data: {e}")

def _calculate_measurement_period(parsed_message: dict) -> int:
    """Calculate measurement period from protobuf data."""
    base = parsed_message.get('measurementPeriodBase', 60)
    factor = parsed_message.get('measurementPeriodFactor', 1)
    return base * factor

def _parse_measurement_channels(parsed_message: dict) -> List[Dict[str, Any]]:
    """Parse measurement channels from Efento protobuf data."""
    channels = []
    measurement_period = _calculate_measurement_period(parsed_message)

    for channel_idx, channel_data in enumerate(parsed_message.get('channels', [])):
        if not channel_data:  # Skip empty channels
            continue

        channel_info = {
            'channel_number': channel_idx + 1,
            'type': channel_data.get('type', '').replace('MEASUREMENT_TYPE_', ''),
            'timestamp': channel_data.get('timestamp'),
            'values': []
        }

        # Process sample offsets
        sample_offsets = channel_data.get('sampleOffsets', [])
        start_point = channel_data.get('startPoint', 0)

        for idx, offset in enumerate(sample_offsets):
            timestamp = channel_data['timestamp'] + (measurement_period * idx)

            # Calculate value based on measurement type
            if channel_data['type'] in ['MEASUREMENT_TYPE_TEMPERATURE', 'MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE']:
                value = (start_point + offset) / 10
            else:
                value = start_point + offset

            channel_info['values'].append({
                'timestamp': timestamp,
                'value': value,
                'offset_index': idx
            })

        channels.append(channel_info)

    return channels

def _parse_device_info(parsed_message: dict) -> Dict[str, Any]:
    """Parse device info from Efento protobuf data."""
    return {
        'serial_number': parsed_message.get('serialNumber', ''),
        'software_version': parsed_message.get('softwareVersion', ''),
        'uptime': parsed_message.get('uptime', 0),
        'battery_voltage': parsed_message.get('batteryVoltage', 0),
        'signal_strength': parsed_message.get('signalStrength', 0)
    }
```

## 7. Configuration Integration

### 7.1 Device Configuration Update

Update device configuration to specify translator:

```yaml
# Device configuration in database
device_id: "a1b2c3d4e5f6g7h8"
parser_file: "efento_measurements_parser.py"
translator: "protobuf_efento" # NEW FIELD
protocol: "coap"
manufacturer: "efento"
```

### 7.2 Normalizer Service Update

```python
# normalizer/service.py

async def _process_translated_data(self, raw_message: RawMessage, translated_data: dict) -> bool:
    """Process data that has been translated from protobuf."""
    try:
        # Get parser configuration
        parser = await self.parser_repository.get_parser_for_sensor(raw_message.device_id)
        if not parser:
            logger.warning(f"No parser found for device: {raw_message.device_id}")
            return True

        # Convert translated data back to JSON string for parser
        translated_payload = json.dumps(translated_data)

        # Execute parser script with translated data
        parsed_results = await self._execute_parser_script(
            parser.file_path,
            translated_payload,  # Use translated data instead of raw hex
            raw_message.metadata
        )

        # Continue with standard normalization flow
        return await self._process_parsed_results(parsed_results, raw_message)

    except Exception as e:
        logger.exception(f"Error processing translated data: {e}")
        return False
```

## 8. Migration Strategy

### 8.1 Phase 1: Schema Update

1. Add `translator` field to `RawMessage` schema
2. Update connectors to set translator field for known devices
3. Deploy schema changes

### 8.2 Phase 2: Translation Layer

1. Implement translation layer components
2. Add Efento protobuf translator
3. Create compiled protobuf modules for Efento

### 8.3 Phase 3: Normalizer Integration

1. Update normalizer to use translation layer
2. Create Efento-specific parser scripts
3. Test with Efento devices

### 8.4 Phase 4: Extension

1. Add support for other manufacturers
2. Create additional protobuf translators
3. Optimize performance

## 9. Benefits of This Design

1. **Backward Compatible**: Existing devices continue to work without changes
2. **Flexible**: Easy to add new manufacturers and protocols
3. **Separation of Concerns**: Translation layer handles protobuf, parsers handle business logic
4. **Reusable**: Translation layer can be used across different services
5. **Maintainable**: Clear separation between protocol parsing and data processing
6. **Configurable**: Device-specific translation via configuration

## 10. Example Data Flow

### 10.1 Efento Device Sending Data

```
1. Efento Device → CoAP Connector
   payload_bytes: [protobuf binary data]

2. CoAP Connector → Kafka
   RawMessage {
     device_id: "a1b2c3d4e5f6g7h8",
     payload_hex: "0a06282c02403cf6...",
     protocol: "coap",
     translator: "protobuf_efento"
   }

3. Normalizer → Translation Layer
   translated_data = {
     "message_type": "measurements",
     "device_id": "a1b2c3d4e5f6g7h8",
     "manufacturer": "efento",
     "parsed_message": {
       "serialNum": "YTFiMmMzZDRlNWY2",
       "batteryStatus": "BATTERY_OK",
       "channels": [...],
       "measurementPeriodBase": 60
     }
   }

4. Normalizer → Parser Script
   parse(json.dumps(translated_data), metadata)

5. Parser Script → StandardizedOutput
   {
     "device_id": "a1b2c3d4e5f6g7h8",
     "values": [25.5, 60.2],
     "index": "measurements",
     "metadata": {
       "manufacturer": "efento",
       "battery_status": "BATTERY_OK"
     }
   }
```

This design provides a clean, maintainable, and extensible way to handle protobuf payloads while preserving the existing normalizer architecture!
