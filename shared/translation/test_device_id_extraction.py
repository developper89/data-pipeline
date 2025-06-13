#!/usr/bin/env python3
"""
Test script for device ID extraction debugging.
"""
import sys
import logging
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging to see our output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

from shared.config_loader import load_connectors_config
from shared.translation.manager import TranslationManager
from shared.models.translation import RawData

def test_device_id_extraction():
    """Test device ID extraction with sample protobuf data."""
    print("🔍 Testing Device ID Extraction with Enhanced Logging\n")
    
    # Load configuration
    config = load_connectors_config()
    coap_config = None
    
    for connector in config.get('connectors', []):
        if connector.get('connector_id') == 'coap_preservarium':
            coap_config = connector
            break
    
    if not coap_config:
        print("❌ CoAP connector configuration not found")
        return
    
    print("✅ Found CoAP connector configuration")
    
    # Initialize translation manager
    print("\n📋 Initializing Translation Manager...")
    translation_manager = TranslationManager(coap_config.get('translators', []))
    
    # Test with some sample binary data (simulating protobuf)
    test_cases = [
        {
            'name': 'Small binary payload',
            'payload': b'\x08\x96\x01\x12\x04test\x18\x01\x20\x00',
            'description': 'Testing with small protobuf-like binary payload'
        },
        {
            'name': 'Larger binary payload',
            'payload': b'\x0a\x08\xab\xcd\xef\x12\x34\x56\x78\x90\x10\x01\x18\x80\x04\x20\x0a' * 3,
            'description': 'Testing with larger protobuf-like binary payload'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🧪 Test Case {i}: {test_case['name']}")
        print(f"📝 {test_case['description']}")
        print(f"📦 Payload size: {len(test_case['payload'])} bytes")
        print(f"🔢 Payload hex: {test_case['payload'].hex()}")
        
        # Create RawData
        raw_data = RawData(
            payload_bytes=test_case['payload'],
            protocol='coap',
            metadata={'source': 'test'}
        )
        
        print("\n📤 Attempting device ID extraction...")
        
        # Try to extract device ID
        result = translation_manager.extract_device_id(raw_data)
        
        print(f"\n📊 Result:")
        print(f"  Success: {result.success}")
        if result.success:
            print(f"  Device ID: {result.device_id}")
            print(f"  Translator: {result.translator_used}")
            print(f"  Metadata: {result.metadata}")
        else:
            print(f"  Error: {result.error}")
            if result.metadata:
                print(f"  Metadata: {result.metadata}")
        
        print("\n" + "="*50)

if __name__ == "__main__":
    test_device_id_extraction() 