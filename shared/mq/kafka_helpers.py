# shared/mq/kafka_helpers.py
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import time
import os
import socket
from datetime import datetime

logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Handle bytes objects (often used in RawMessage.payload)
        elif isinstance(obj, bytes):
            return obj.hex()  # Convert bytes to hex string
        # Let the base class handle the rest
        return super().default(obj)

def model_to_dict(model):
    """Convert a Pydantic model to a dictionary with proper datetime handling."""
    if hasattr(model, 'model_dump'):  # Pydantic v2
        return model.model_dump()
    else:  # Pydantic v1
        return model.dict()

def validate_bootstrap_servers(bootstrap_servers):
    """Validate that bootstrap servers are reachable."""
    servers = bootstrap_servers.split(',')
    reachable_servers = []
    
    for server in servers:
        server = server.strip()
        try:
            if ':' in server:
                host, port = server.split(':')
                port = int(port)
            else:
                host = server
                port = 9092
            
            # Test DNS resolution
            try:
                socket.gethostbyname(host)
                logger.debug(f"DNS resolution successful for {host}")
            except socket.gaierror as e:
                logger.warning(f"DNS resolution failed for {host}: {e}")
                # Try common fallbacks for Docker environments
                if host == 'kafka':
                    fallback_hosts = ['localhost', '127.0.0.1']
                    for fallback in fallback_hosts:
                        try:
                            socket.gethostbyname(fallback)
                            logger.info(f"Using fallback {fallback}:{port} instead of {host}:{port}")
                            server = f"{fallback}:{port}"
                            host = fallback
                            break
                        except socket.gaierror:
                            continue
                    else:
                        logger.error(f"No fallback found for {host}")
                        continue
                else:
                    continue
            
            # Test port connectivity
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    reachable_servers.append(server)
                    logger.debug(f"Successfully connected to {host}:{port}")
                else:
                    logger.warning(f"Cannot connect to {host}:{port} (error code: {result})")
            except Exception as e:
                logger.warning(f"Connection test failed for {host}:{port}: {e}")
                
        except Exception as e:
            logger.error(f"Error validating server {server}: {e}")
    
    if not reachable_servers:
        logger.error(f"No reachable Kafka brokers found in: {bootstrap_servers}")
        return None
    
    validated_servers = ','.join(reachable_servers)
    if validated_servers != bootstrap_servers:
        logger.info(f"Updated bootstrap servers from {bootstrap_servers} to {validated_servers}")
    
    return validated_servers

def create_kafka_producer(bootstrap_servers, base_delay=1):
    """Creates a KafkaProducer instance with enhanced error handling and infinite retry logic."""
    # Validate servers first
    validated_servers = validate_bootstrap_servers(bootstrap_servers)
    if not validated_servers:
        logger.warning(f"No reachable Kafka brokers found in: {bootstrap_servers}. Will keep retrying...")
        validated_servers = bootstrap_servers  # Use original servers and keep trying
    
    producer = None
    retry_count = 0
    
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=validated_servers.split(','),
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
                retries=5,  # Retry sending messages
                acks='all',  # Wait for all replicas to acknowledge
                request_timeout_ms=40000,  # 40 seconds (must be > session_timeout_ms)
                metadata_max_age_ms=30000,  # Refresh metadata every 30 seconds
                reconnect_backoff_ms=1000,  # Wait 1 second before reconnecting
                retry_backoff_ms=100,  # Wait 100ms between retries
                max_block_ms=10000,  # Max time to block on send()
                api_version_auto_timeout_ms=10000,  # API version detection timeout
                security_protocol='PLAINTEXT'
            )
            logger.info(f"KafkaProducer connected to {validated_servers}")
            return producer
            
        except NoBrokersAvailable as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))  # Exponential backoff, max 64 seconds
            logger.error(f"No Kafka brokers available (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except KafkaError as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Kafka error creating producer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except Exception as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Unexpected error creating Kafka Producer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)


def create_kafka_consumer(topic, group_id, bootstrap_servers, auto_offset_reset='earliest', base_delay=1):
    """Creates a KafkaConsumer instance with enhanced error handling and infinite retry logic."""
    # Validate servers first
    validated_servers = validate_bootstrap_servers(bootstrap_servers)
    if not validated_servers:
        logger.warning(f"No reachable Kafka brokers found in: {bootstrap_servers}. Will keep retrying...")
        validated_servers = bootstrap_servers  # Use original servers and keep trying
    
    consumer = None
    retry_count = 0
    
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic,
                group_id=group_id,
                bootstrap_servers=validated_servers.split(','),
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset=auto_offset_reset,  # 'earliest' or 'latest'
                enable_auto_commit=False,  # Important for processing guarantees
                request_timeout_ms=40000,  # 40 seconds (must be > session_timeout_ms)
                session_timeout_ms=30000,  # 30 seconds
                heartbeat_interval_ms=10000,  # 10 seconds (should be < session_timeout_ms/3)
                metadata_max_age_ms=30000,  # Refresh metadata every 30 seconds
                reconnect_backoff_ms=1000,  # Wait 1 second before reconnecting
                retry_backoff_ms=100,  # Wait 100ms between retries
                api_version_auto_timeout_ms=10000,  # API version detection timeout
                security_protocol='PLAINTEXT',
                consumer_timeout_ms=1000  # Timeout for consumer.poll()
            )
            logger.info(f"KafkaConsumer connected to {validated_servers} for topic '{topic}', group '{group_id}'")
            return consumer
            
        except NoBrokersAvailable as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"No Kafka brokers available for consumer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except KafkaError as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Kafka error creating consumer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except Exception as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Unexpected error creating Kafka Consumer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)


def publish_message(producer: KafkaProducer, topic: str, value: dict, key: str = None):
    """Publishes a message to a Kafka topic with enhanced error handling."""
    try:
        # Convert Pydantic models to dict if needed
        if hasattr(value, '__dict__') and hasattr(value, 'model_dump'):
            # This is likely a Pydantic model
            value = model_to_dict(value)

        key_bytes = key.encode('utf-8') if key else None
        future = producer.send(topic, value=value, key=key_bytes)
        
        # Wait for the message to be sent with timeout
        try:
            record_metadata = future.get(timeout=10)
            logger.debug(f"Published message to Kafka topic '{topic}', partition {record_metadata.partition}, offset {record_metadata.offset}")
        except Exception as e:
            logger.warning(f"Failed to get send confirmation for topic '{topic}': {e}")
            # Still flush to ensure message is sent
            producer.flush(timeout=5)
            logger.debug(f"Flushed message to Kafka topic '{topic}' with key '{key}'")

    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error publishing to Kafka topic '{topic}': {e}")
        raise

def check_kafka_health(bootstrap_servers):
    """Check if Kafka brokers are healthy and reachable."""
    try:
        validated_servers = validate_bootstrap_servers(bootstrap_servers)
        if not validated_servers:
            return False, "No reachable Kafka brokers found"
        
        # Try to create a temporary consumer to test connectivity
        test_consumer = KafkaConsumer(
            bootstrap_servers=validated_servers.split(','),
            consumer_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
            security_protocol='PLAINTEXT'
        )
        
        # Get metadata to verify connection
        metadata = test_consumer.list_consumer_groups()
        test_consumer.close()
        
        return True, f"Successfully connected to Kafka brokers: {validated_servers}"
        
    except Exception as e:
        return False, f"Kafka health check failed: {e}"