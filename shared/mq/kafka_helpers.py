# shared/mq/kafka_helpers.py
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import time
import os

logger = logging.getLogger(__name__)

def create_kafka_producer(bootstrap_servers):
    """Creates a KafkaProducer instance with basic error handling."""
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5, # Retry sending messages
                acks='all' # Wait for all replicas to acknowledge
            )
            logger.info(f"KafkaProducer connected to {bootstrap_servers}")
            return producer
        except KafkaError as e:
            logger.error(f"Failed to connect Kafka Producer to {bootstrap_servers}: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
             logger.error(f"Unexpected error creating Kafka Producer: {e}. Retrying in 5 seconds...")
             time.sleep(5)


def create_kafka_consumer(topic, group_id, bootstrap_servers, auto_offset_reset='earliest'):
    """Creates a KafkaConsumer instance."""
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic,
                group_id=group_id,
                bootstrap_servers=bootstrap_servers.split(','),
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset=auto_offset_reset, # 'earliest' or 'latest'
                enable_auto_commit=False # Important for processing guarantees
            )
            logger.info(f"KafkaConsumer connected to {bootstrap_servers} for topic '{topic}', group '{group_id}'")
            return consumer
        except KafkaError as e:
             logger.error(f"Failed to connect Kafka Consumer to {bootstrap_servers} for topic {topic}: {e}. Retrying in 5 seconds...")
             time.sleep(5)
        except Exception as e:
             logger.error(f"Unexpected error creating Kafka Consumer: {e}. Retrying in 5 seconds...")
             time.sleep(5)


def publish_message(producer: KafkaProducer, topic: str, value: dict, key: str = None):
    """Publishes a message to a Kafka topic."""
    try:
        key_bytes = key.encode('utf-8') if key else None
        future = producer.send(topic, value=value, key=key_bytes)
        # Optional: Block until message is sent (synchronous)
        # record_metadata = future.get(timeout=10)
        # logger.debug(f"Published message to Kafka topic '{topic}', partition {record_metadata.partition}, offset {record_metadata.offset}")
        # Or handle asynchronously using callbacks (add_callback, add_errback)
        producer.flush(timeout=5) # Attempt to send buffered messages
        logger.debug(f"Attempted flush for message to Kafka topic '{topic}' with key '{key}'")

    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        # Implement retry logic or raise exception as needed
        raise
    except Exception as e:
        logger.exception(f"Unexpected error publishing to Kafka topic '{topic}': {e}")
        raise