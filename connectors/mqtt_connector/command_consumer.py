# mqtt_connector/command_consumer.py
import logging
import json
import threading
from typing import Dict, Any, Optional
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import create_kafka_consumer
from client import MQTTClientWrapper
import config

logger = logging.getLogger(__name__)

class CommandConsumer:
    """
    Consumes device command messages from Kafka and publishes them to MQTT topics.
    Runs in a separate thread to avoid blocking the MQTT client loop.
    """
    
    def __init__(self, mqtt_client: MQTTClientWrapper):
        """
        Initialize the command consumer.
        
        Args:
            mqtt_client: The MQTT client wrapper to use for publishing
        """
        self.mqtt_client = mqtt_client
        self.consumer = None
        self.running = False
        self.thread = None

    def start(self):
        """Start the command consumer thread."""
        if self.running:
            logger.warning("Command consumer thread already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()
        logger.info("Command consumer thread started")

    def stop(self):
        """Stop the command consumer thread."""
        if not self.running:
            logger.warning("Command consumer thread not running")
            return
            
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
            self.thread = None
            
        self._close_consumer()
        logger.info("Command consumer thread stopped")

    def _consume_loop(self):
        """Main loop for consuming command messages."""
        retry_delay = 5  # Initial retry delay in seconds
        max_retry_delay = 60  # Maximum retry delay in seconds
        
        while self.running:
            try:
                # Create the consumer if it doesn't exist
                if not self.consumer:
                    self.consumer = create_kafka_consumer(
                        topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
                        group_id=f"{config.MQTT_CLIENT_ID}_command_consumer",
                        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                        auto_offset_reset='latest'  # Only consume new messages
                    )
                    if not self.consumer:
                        logger.error("Failed to create Kafka consumer, will retry...")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff
                        continue
                    
                    retry_delay = 5  # Reset retry delay on successful connection
                
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    # No messages, continue polling
                    time.sleep(0.1)
                    continue
                
                # Process received messages
                for tp, messages in message_batch.items():
                    for message in messages:
                        if not self.running:
                            break
                            
                        self._process_command(message.value)
                
                # Commit offsets
                self.consumer.commit()
                    
            except KafkaError as e:
                logger.error(f"Kafka error in command consumer: {e}")
                self._close_consumer()
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                logger.exception(f"Unexpected error in command consumer: {e}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
    
    def _process_command(self, command_data: Dict[str, Any]):
        """
        Process a command message and publish it to MQTT.
        
        Args:
            command_data: The command data from Kafka
        """
        try:
            # Check if this is an MQTT command
            if command_data.get('protocol', '').lower() != 'mqtt':
                logger.debug(f"Ignoring non-MQTT command: {command_data.get('protocol')}")
                return
                
            # Extract required fields
            device_id = command_data.get('device_id')
            payload = command_data.get('payload')
            request_id = command_data.get('request_id', 'unknown')
            metadata = command_data.get('metadata', {})
            
            # Extract topic from metadata (required for MQTT)
            topic = metadata.get('topic', None)
            if not topic:
                logger.error(f"[{request_id}] MQTT command missing topic in metadata for device {device_id}")
                return
            
            # Extract optional MQTT-specific fields from metadata
            qos = metadata.get('qos', None)
            retain = metadata.get('retain', None)
            
            if not device_id or payload is None:
                logger.error(f"[{request_id}] Invalid command message: missing required fields")
                return
                
            logger.info(f"[{request_id}] Processing command for device {device_id} on topic {topic}")
            
            # Publish the message to MQTT
            success = self.mqtt_client.publish_message(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain
            )
            
            if success:
                logger.info(f"[{request_id}] Successfully published command to {topic}")
            else:
                logger.error(f"[{request_id}] Failed to publish command to {topic}")
                
        except Exception as e:
            logger.exception(f"Error processing command: {e}")
    
    def _close_consumer(self):
        """Close the Kafka consumer if it exists."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka command consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None 