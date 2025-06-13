# shared/translation/examples/efento_example.py

"""
Example usage of the translation layer with Efento protobuf translator.

This script demonstrates how to:
1. Configure the ProtobufTranslator for Efento devices
2. Create a TranslationManager 
3. Extract device IDs from protobuf payloads
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from shared.translation import ProtobufTranslator, TranslationManager
from shared.models.translation import RawData

def create_efento_translator():
    """Create a configured ProtobufTranslator for Efento devices."""
    
    efento_config = {
        "manufacturer": "efento",
        "device_id_extraction": {
            "sources": [
                {
                    "message_type": "measurements", 
                    "field_path": "serialNum", 
                    "priority": 1
                },
                {
                    "message_type": "device_info",
                    "field_path": "imei", 
                    "priority": 2
                }
            ],
            "validation": {
                "regex": r"^[a-f0-9]{16}$",
                "normalize": False
            }
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
    
    return ProtobufTranslator(efento_config)

def main():
    """Example usage of the translation layer."""
    
    try:
        # Create Efento translator
        efento_translator = create_efento_translator()
        
        # Create translation manager
        translation_manager = TranslationManager([efento_translator])
        
        print(f"Translation manager initialized with {translation_manager.get_translator_count()} translator(s)")
        
        # Example protobuf payload (hex-encoded)
        # This would come from your CoAP connector in practice
        example_payload_hex = "0a06282c02403cf61001183c22170801108c9cfc860620a2042a0a0100000101000100000022160802108c9cfc860620602a0a000103030100010201002885a8fc8606381a4001482582010f383637393937303331393539343133"
        
        # Convert hex to bytes
        payload_bytes = bytes.fromhex(example_payload_hex)
        
        # Create RawData object
        raw_data = RawData(
            payload_bytes=payload_bytes,
            protocol="coap",
            metadata={"source": "example"}
        )
        
        # Extract device ID
        result = translation_manager.extract_device_id(raw_data)
        
        if result.success:
            print(f"✅ Device ID extracted: {result.device_id}")
            print(f"   Translator used: {result.translator_used}")
            print(f"   Metadata: {result.metadata}")
        else:
            print(f"❌ Failed to extract device ID: {result.error}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Note: Make sure Efento protobuf modules are compiled and available")

if __name__ == "__main__":
    main() 