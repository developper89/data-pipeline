#!/usr/bin/env python3
"""
Test script for protocol-agnostic pattern-based device ID translation.

This script tests the PatternTranslator with various protocols:
- MQTT topics
- CoAP resource paths  
- HTTP URLs
- JSON payloads
"""

import json
import logging
import sys
import os

# Add the shared directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from shared.models.translation import RawData
from shared.translation.pattern.translator import PatternTranslator

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_pattern_translator():
    """Test the protocol-agnostic pattern translator with various scenarios."""
    
    logger.info("=== Starting Pattern Translator Tests ===")
    
    # Test configuration with only path_pattern and json_payload types
    config = {
        'device_id_extraction': {
            'sources': [
                # Path patterns for different protocols
                {
                    'type': 'path_pattern',
                    'pattern': 'broker_data/{device_id}/data',
                    'priority': 1,
                    'validation': {
                        'min_length': 3,
                        'max_length': 50,
                        'pattern': '^[a-fA-F0-9]+$'
                    }
                },
                {
                    'type': 'path_pattern', 
                    'pattern': 'sensors/{device_id}/measurements',
                    'priority': 2,
                    'validation': {
                        'min_length': 3,
                        'max_length': 50
                    }
                },
                {
                    'type': 'path_pattern',
                    'pattern': '/devices/{device_id}/readings',
                    'priority': 3,
                    'validation': {
                        'min_length': 3,
                        'max_length': 50
                    }
                },
                {
                    'type': 'path_pattern',
                    'pattern': '/api/v1/devices/{device_id}/status',
                    'priority': 4,
                    'validation': {
                        'min_length': 3,
                        'max_length': 50
                    }
                },
                # JSON payload extraction
                {
                    'type': 'json_payload',
                    'json_path': '$.device.id',
                    'fallback_paths': ['$.deviceId', '$.id', '$.device_id'],
                    'priority': 5,
                    'validation': {
                        'min_length': 3,
                        'max_length': 50
                    }
                }
            ]
        }
    }
    
    # Create translator
    translator = PatternTranslator(config)
    
    # Test cases for different protocols
    test_cases = [
        # MQTT topic patterns
        {
            'name': 'MQTT Topic - broker_data pattern',
            'raw_data': RawData(
                payload_bytes=b'{"temperature": 23.5}',
                protocol='mqtt',
                metadata={'topic': 'broker_data/abc123/data'}
            ),
            'expected_device_id': 'abc123'
        },
        {
            'name': 'MQTT Topic - sensors pattern',
            'raw_data': RawData(
                payload_bytes=b'{"humidity": 65}',
                protocol='mqtt', 
                metadata={'topic': 'sensors/def456/measurements'}
            ),
            'expected_device_id': 'def456'
        },
        # CoAP resource paths
        {
            'name': 'CoAP Resource Path',
            'raw_data': RawData(
                payload_bytes=b'protobuf_data_here',
                protocol='coap',
                path='/devices/789abc/readings'
            ),
            'expected_device_id': '789abc'
        },
        # HTTP URL paths
        {
            'name': 'HTTP API Path',
            'raw_data': RawData(
                payload_bytes=b'{"status": "online"}',
                protocol='http',
                path='/api/v1/devices/fedcba987/status'
            ),
            'expected_device_id': 'fedcba987'
        },
        # JSON payload extraction
        {
            'name': 'JSON Payload - device.id path',
            'raw_data': RawData(
                payload_bytes=json.dumps({
                    'device': {'id': 'json123'},
                    'temperature': 24.0
                }).encode('utf-8'),
                protocol='http',
                path='/api/unknown/path'
            ),
            'expected_device_id': 'json123'
        },
        {
            'name': 'JSON Payload - fallback to deviceId',
            'raw_data': RawData(
                payload_bytes=json.dumps({
                    'deviceId': 'fallback456',
                    'humidity': 60
                }).encode('utf-8'),
                protocol='mqtt',
                metadata={'topic': 'unknown/topic/structure'}
            ),
            'expected_device_id': 'fallback456'
        },
        # Edge cases
        {
            'name': 'No matching pattern',
            'raw_data': RawData(
                payload_bytes=b'{"data": "test"}',
                protocol='mqtt',
                metadata={'topic': 'unknown/topic/format'}
            ),
            'expected_device_id': None
        },
        {
            'name': 'Invalid device ID (validation failure)',
            'raw_data': RawData(
                payload_bytes=b'{"temperature": 25}',
                protocol='mqtt',
                metadata={'topic': 'broker_data/invalid-device-id-with-dashes/data'}
            ),
            'expected_device_id': None  # Should fail hex validation
        }
    ]
    
    # Run test cases
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Test {i}: {test_case['name']} ---")
        
        try:
            # Extract device ID
            result = translator.extract_device_id(test_case['raw_data'])
            expected = test_case['expected_device_id']
            
            # Check result
            if result == expected:
                logger.info(f"✅ PASS: Expected '{expected}', got '{result}'")
                passed += 1
            else:
                logger.error(f"❌ FAIL: Expected '{expected}', got '{result}'")
                failed += 1
                
        except Exception as e:
            logger.error(f"❌ ERROR: {e}")
            failed += 1
    
    # Summary
    total = passed + failed
    logger.info(f"\n=== Test Summary ===")
    logger.info(f"Total tests: {total}")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Success rate: {(passed/total)*100:.1f}%")
    
    if failed == 0:
        logger.info("🎉 All tests passed!")
        return True
    else:
        logger.error(f"💥 {failed} test(s) failed!")
        return False


def test_translation_result():
    """Test the full translation workflow."""
    
    logger.info("\n=== Testing Translation Result ===")
    
    config = {
        'device_id_extraction': {
            'sources': [
                {
                    'type': 'path_pattern',
                    'pattern': 'test/{device_id}',
                    'priority': 1
                }
            ]
        }
    }
    
    translator = PatternTranslator(config)
    
    raw_data = RawData(
        payload_bytes=b'test data',
        protocol='test',
        path='test/device123'
    )
    
    result = translator.translate(raw_data)
    
    logger.info(f"Translation result:")
    logger.info(f"  Success: {result.success}")
    logger.info(f"  Device ID: {result.device_id}")
    logger.info(f"  Translator type: {result.translator_type}")
    logger.info(f"  Metadata: {result.metadata}")
    
    return result.success and result.device_id == 'device123'


if __name__ == '__main__':
    logger.info("Starting Protocol-Agnostic Pattern Translation Tests")
    
    success1 = test_pattern_translator()
    success2 = test_translation_result()
    
    if success1 and success2:
        logger.info("\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("\n💥 Some tests failed!")
        sys.exit(1) 