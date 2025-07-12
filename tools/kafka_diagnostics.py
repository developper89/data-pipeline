#!/usr/bin/env python3
"""
Kafka Diagnostics Tool
Helps diagnose and troubleshoot Kafka connection and consumer group issues.
"""

import sys
import os
import time
import json
import logging
from datetime import datetime

# Add the parent directory to the path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.mq.kafka_helpers import (
    validate_bootstrap_servers, 
    create_kafka_consumer, 
    create_kafka_producer,
    check_kafka_health,
    get_kafka_cluster_info
)
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_basic_connectivity(bootstrap_servers):
    """Test basic connectivity to Kafka brokers."""
    print("\n=== Testing Basic Connectivity ===")
    
    # Test server validation
    validated_servers = validate_bootstrap_servers(bootstrap_servers)
    if validated_servers:
        print(f"✓ Successfully validated servers: {validated_servers}")
    else:
        print(f"✗ Failed to validate servers: {bootstrap_servers}")
        return False
    
    # Test health check
    healthy, message = check_kafka_health(bootstrap_servers)
    if healthy:
        print(f"✓ Kafka health check passed: {message}")
    else:
        print(f"✗ Kafka health check failed: {message}")
        return False
    
    return True

def test_cluster_info(bootstrap_servers):
    """Get detailed cluster information."""
    print("\n=== Cluster Information ===")
    
    cluster_info = get_kafka_cluster_info(bootstrap_servers)
    if cluster_info:
        print(f"✓ Bootstrap servers: {cluster_info['bootstrap_servers']}")
        print(f"✓ Available topics: {cluster_info['topics']}")
        print(f"✓ Consumer groups: {cluster_info['consumer_groups']}")
        
        print("\n--- Topic Partitions ---")
        for topic, partitions in cluster_info['partitions'].items():
            print(f"  {topic}: {len(partitions)} partitions")
            
        return cluster_info
    else:
        print("✗ Failed to get cluster information")
        return None

def test_producer_performance(bootstrap_servers, topic="test-topic", num_messages=10):
    """Test producer performance and reliability."""
    print(f"\n=== Testing Producer Performance ===")
    print(f"Sending {num_messages} messages to topic '{topic}'...")
    
    try:
        producer = create_kafka_producer(bootstrap_servers)
        
        start_time = time.time()
        success_count = 0
        
        for i in range(num_messages):
            try:
                message = {
                    'message_id': i,
                    'timestamp': datetime.now().isoformat(),
                    'test_data': f'Test message {i}'
                }
                
                future = producer.send(topic, value=message)
                # Wait for delivery with timeout
                record_metadata = future.get(timeout=10)
                success_count += 1
                
                print(f"  Message {i}: ✓ Sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
                
            except Exception as e:
                print(f"  Message {i}: ✗ Failed to send: {e}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n--- Producer Results ---")
        print(f"  Success rate: {success_count}/{num_messages} ({success_count/num_messages*100:.1f}%)")
        print(f"  Total time: {duration:.2f} seconds")
        print(f"  Average time per message: {duration/num_messages:.3f} seconds")
        
        producer.close()
        return success_count == num_messages
        
    except Exception as e:
        print(f"✗ Producer test failed: {e}")
        return False

def test_consumer_group_stability(bootstrap_servers, topic, group_id, duration=30):
    """Test consumer group stability and heartbeat issues."""
    print(f"\n=== Testing Consumer Group Stability ===")
    print(f"Testing consumer group '{group_id}' for {duration} seconds...")
    
    try:
        consumer = create_kafka_consumer(topic, group_id, bootstrap_servers)
        
        start_time = time.time()
        message_count = 0
        error_count = 0
        last_heartbeat = time.time()
        
        while time.time() - start_time < duration:
            try:
                message_batch = consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        message_count += len(messages)
                        print(f"  Received {len(messages)} messages from {topic_partition}")
                        
                        # Commit offsets
                        for message in messages:
                            consumer.commit({topic_partition: message.offset + 1})
                
                # Check heartbeat interval
                current_time = time.time()
                if current_time - last_heartbeat >= 5:  # Every 5 seconds
                    print(f"  Heartbeat check: {current_time - start_time:.1f}s elapsed")
                    last_heartbeat = current_time
                
            except KafkaError as e:
                error_count += 1
                print(f"  ✗ Kafka error: {e}")
                
            except Exception as e:
                error_count += 1
                print(f"  ✗ Unexpected error: {e}")
        
        consumer.close()
        
        print(f"\n--- Consumer Group Results ---")
        print(f"  Messages received: {message_count}")
        print(f"  Errors encountered: {error_count}")
        print(f"  Test duration: {duration} seconds")
        
        return error_count == 0
        
    except Exception as e:
        print(f"✗ Consumer group test failed: {e}")
        return False

def analyze_consumer_group_issues(bootstrap_servers, group_id):
    """Analyze specific consumer group issues."""
    print(f"\n=== Analyzing Consumer Group Issues ===")
    
    try:
        # Create a temporary consumer to check group status
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(','),
            group_id=group_id,
            consumer_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
            security_protocol='PLAINTEXT'
        )
        
        # Get group information
        try:
            group_info = consumer.list_consumer_groups()
            print(f"Available consumer groups: {[g.group_id for g in group_info]}")
            
            # Check if our group exists
            our_group = [g for g in group_info if g.group_id == group_id]
            if our_group:
                print(f"✓ Consumer group '{group_id}' exists")
            else:
                print(f"⚠ Consumer group '{group_id}' not found in active groups")
                
        except Exception as e:
            print(f"⚠ Could not get consumer group info: {e}")
        
        consumer.close()
        
    except Exception as e:
        print(f"✗ Consumer group analysis failed: {e}")

def run_comprehensive_diagnostics(bootstrap_servers, topic=None, group_id=None):
    """Run comprehensive Kafka diagnostics."""
    print("🔍 Kafka Diagnostics Tool")
    print("=" * 50)
    
    results = {}
    
    # Test 1: Basic connectivity
    results['connectivity'] = test_basic_connectivity(bootstrap_servers)
    
    # Test 2: Cluster information
    cluster_info = test_cluster_info(bootstrap_servers)
    results['cluster_info'] = cluster_info is not None
    
    # Test 3: Producer performance
    if topic:
        results['producer'] = test_producer_performance(bootstrap_servers, topic)
    
    # Test 4: Consumer group stability
    if topic and group_id:
        results['consumer_stability'] = test_consumer_group_stability(
            bootstrap_servers, topic, group_id
        )
        
        # Test 5: Consumer group analysis
        analyze_consumer_group_issues(bootstrap_servers, group_id)
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 DIAGNOSTICS SUMMARY")
    print("=" * 50)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
    
    all_passed = all(results.values())
    if all_passed:
        print("\n🎉 All tests passed! Your Kafka setup looks healthy.")
    else:
        print("\n⚠️  Some tests failed. Check the details above for troubleshooting.")
    
    return all_passed

def main():
    parser = argparse.ArgumentParser(description='Kafka Diagnostics Tool')
    parser.add_argument(
        '--bootstrap-servers', 
        default='kafka:9092',
        help='Kafka bootstrap servers (default: kafka:9092)'
    )
    parser.add_argument(
        '--topic',
        default='test-topic',
        help='Topic to use for testing (default: test-topic)'
    )
    parser.add_argument(
        '--group-id',
        default='diagnostic-group',
        help='Consumer group ID for testing (default: diagnostic-group)'
    )
    parser.add_argument(
        '--test-duration',
        type=int,
        default=30,
        help='Duration for consumer stability test in seconds (default: 30)'
    )
    
    args = parser.parse_args()
    
    try:
        success = run_comprehensive_diagnostics(
            args.bootstrap_servers,
            args.topic,
            args.group_id
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\nDiagnostics interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nDiagnostics failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 