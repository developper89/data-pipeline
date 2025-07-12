#!/usr/bin/env python3
"""
Quick test script to verify Kafka improvements are working.
"""

import sys
import os
import time
import logging
from datetime import datetime

# Add the parent directory to the path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.mq.kafka_helpers import (
    create_kafka_consumer, 
    create_kafka_producer,
    safe_kafka_poll,
    safe_kafka_commit,
    is_consumer_healthy,
    check_kafka_health,
    get_optimized_consumer_config,
    get_optimized_producer_config
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_improved_configurations():
    """Test that the improved configurations have the expected settings."""
    print("\n=== Testing Improved Configurations ===")
    
    # Test consumer config
    consumer_config = get_optimized_consumer_config("test-group")
    
    expected_consumer_settings = {
        'request_timeout_ms': 120000,
        'session_timeout_ms': 90000,
        'heartbeat_interval_ms': 30000,
        'metadata_max_age_ms': 60000,
        'reconnect_backoff_ms': 2000,
        'max_poll_interval_ms': 600000
    }
    
    print("✓ Consumer Configuration:")
    for key, expected_value in expected_consumer_settings.items():
        actual_value = consumer_config.get(key)
        if actual_value == expected_value:
            print(f"  ✅ {key}: {actual_value}")
        else:
            print(f"  ❌ {key}: {actual_value} (expected {expected_value})")
    
    # Test producer config
    producer_config = get_optimized_producer_config()
    
    expected_producer_settings = {
        'request_timeout_ms': 120000,
        'retries': 10,
        'metadata_max_age_ms': 60000,
        'reconnect_backoff_ms': 2000,
        'delivery_timeout_ms': 300000
    }
    
    print("\n✓ Producer Configuration:")
    for key, expected_value in expected_producer_settings.items():
        actual_value = producer_config.get(key)
        if actual_value == expected_value:
            print(f"  ✅ {key}: {actual_value}")
        else:
            print(f"  ❌ {key}: {actual_value} (expected {expected_value})")

def test_safe_utilities():
    """Test the safe utility functions."""
    print("\n=== Testing Safe Utility Functions ===")
    
    try:
        # Test creating consumer with improved config
        consumer = create_kafka_consumer(
            topic="test-topic",
            group_id="test-group",
            bootstrap_servers="kafka:9092"
        )
        
        if consumer:
            print("✅ Successfully created consumer with improved config")
            
            # Test safe polling
            print("✅ Testing safe polling...")
            message_batch = safe_kafka_poll(consumer, timeout_ms=1000)
            print(f"✅ Safe poll returned: {type(message_batch)} (no exceptions)")
            
            # Test health check
            print("✅ Testing health check...")
            is_healthy = is_consumer_healthy(consumer)
            print(f"✅ Consumer health check: {'Healthy' if is_healthy else 'Unhealthy'}")
            
            # Test safe commit
            print("✅ Testing safe commit...")
            commit_result = safe_kafka_commit(consumer)
            print(f"✅ Safe commit result: {'Success' if commit_result else 'Failed'}")
            
            consumer.close()
        else:
            print("❌ Failed to create consumer")
            
    except Exception as e:
        print(f"❌ Error testing safe utilities: {e}")

def test_connection_improvements():
    """Test that connection improvements are working."""
    print("\n=== Testing Connection Improvements ===")
    
    # Test health check
    healthy, message = check_kafka_health("kafka:9092")
    if healthy:
        print(f"✅ Kafka health check passed: {message}")
    else:
        print(f"❌ Kafka health check failed: {message}")
    
    # Test producer creation with improved config
    try:
        producer = create_kafka_producer("kafka:9092")
        if producer:
            print("✅ Successfully created producer with improved config")
            
            # Test sending a message
            test_message = {
                'timestamp': datetime.now().isoformat(),
                'test_data': 'Connection test message',
                'source': 'test_kafka_improvements.py'
            }
            
            try:
                future = producer.send('test-topic', value=test_message)
                # Don't wait for delivery, just test that send doesn't throw
                print("✅ Successfully sent test message")
            except Exception as e:
                print(f"⚠️ Could not send test message: {e}")
            
            producer.close()
        else:
            print("❌ Failed to create producer")
            
    except Exception as e:
        print(f"❌ Error testing producer: {e}")

def main():
    """Run all tests."""
    print("🧪 Testing Kafka Improvements")
    print("=" * 50)
    
    test_improved_configurations()
    test_safe_utilities()
    test_connection_improvements()
    
    print("\n" + "=" * 50)
    print("✅ Tests completed!")
    print("\n📝 Next steps:")
    print("1. Monitor your services for reduced connection errors")
    print("2. Run the full diagnostics: python tools/kafka_diagnostics.py")
    print("3. Check service logs for improved reconnection behavior")

if __name__ == "__main__":
    main() 