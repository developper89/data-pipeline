# mqtt_connector/command_consumer.py
import logging
import json
import asyncio
from typing import Dict, Any, Optional

from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer
from client import MQTTClientWrapper
import config

logger = logging.getLogger(__name__)

class CommandConsumer:
    """
    Consumes device command messages from Kafka and publishes them to MQTT topics.
    Uses AsyncResilientKafkaConsumer for automatic error handling and reconnection.
    """
    
    def __init__(self, mqtt_client: MQTTClientWrapper):
        """
        Initialize the command consumer.
        
        Args:
            mqtt_client: The MQTT client wrapper to use for publishing
        """
        self.mqtt_client = mqtt_client
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        self.running = False
        self._consumer_task = None

    async def start(self):
        """Start the command consumer with AsyncResilientKafkaConsumer."""
        if self.running:
            logger.warning("Command consumer already running")
            return
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
            group_id=f"{config.MQTT_CLIENT_ID}_command_consumer",
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only consume new messages
            on_error_callback=self._on_consumer_error
        )
            
        self.running = True
        
        # Start consuming in a separate asyncio task
        self._consumer_task = asyncio.create_task(self._consume_commands())
        
        logger.info("Async command consumer started")

    async def stop(self):
        """Stop the command consumer."""
        if not self.running:
            logger.warning("Command consumer not running")
            return
            
        self.running = False
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
            
        # Wait for consumer task to finish
        if self._consumer_task and not self._consumer_task.done():
            try:
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Command consumer task did not finish within timeout")
                self._consumer_task.cancel()
                try:
                    await self._consumer_task
                except asyncio.CancelledError:
                    pass
            
        logger.info("Async command consumer stopped")

    async def _consume_commands(self):
        """Start consuming command messages using AsyncResilientKafkaConsumer."""
        if not self.resilient_consumer:
            logger.error("Cannot start consuming: AsyncResilientKafkaConsumer not initialized")
            return
            
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_command_message,
                commit_offset=True
            )
        except Exception as e:
            logger.exception(f"Fatal error in command consumer: {e}")
            self.running = False

    async def _process_command_message(self, message):
        """
        Process a single command message.
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Process the command
            success = await self._process_command(message.value)
            return success
            
        except Exception as e:
            logger.exception(f"Unexpected error processing command message: {e}")
            return False  # Don't commit offset on error
    
    async def _on_consumer_error(self, error, context):
        """Handle consumer-level errors."""
        logger.error(f"Consumer error: {error}")
        # Could implement additional error handling logic here if needed
    
    async def _process_command(self, command_data: Dict[str, Any]) -> bool:
        """
        Process a command message and publish it to MQTT.
        
        Args:
            command_data: The command data from Kafka
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Check if this is an MQTT command
            if command_data.get('protocol', '').lower() != 'mqtt':
                logger.debug(f"Ignoring non-MQTT command: {command_data.get('protocol')}")
                return True  # Successfully ignored
                
            # Extract required fields
            device_id = command_data.get('device_id')
            payload = command_data.get('payload')
            request_id = command_data.get('request_id', 'unknown')
            metadata = command_data.get('metadata', {})
            
            # Extract topic from metadata (required for MQTT)
            topic = metadata.get('topic', None)
            if not topic:
                logger.error(f"[{request_id}] MQTT command missing topic in metadata for device {device_id}")
                return False
            
            # Extract optional MQTT-specific fields from metadata
            qos = metadata.get('qos', None)
            retain = metadata.get('retain', None)
            
            if not device_id or payload is None:
                logger.error(f"[{request_id}] Invalid command message: missing required fields")
                return False
                
            logger.info(f"[{request_id}] Processing command for device {device_id} on topic {topic}")
            
            # Publish the message to MQTT using executor for the synchronous call
            success = await asyncio.get_event_loop().run_in_executor(
                None,
                self.mqtt_client.publish_message,
                topic,
                payload,
                qos,
                retain
            )
            
            if success:
                logger.info(f"[{request_id}] Successfully published command to {topic}")
                return True
            else:
                logger.error(f"[{request_id}] Failed to publish command to {topic}")
                return False
                
        except Exception as e:
            logger.exception(f"Error processing command: {e}")
            return False 