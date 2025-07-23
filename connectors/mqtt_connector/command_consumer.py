# mqtt_connector/command_consumer.py
import logging
import time
import asyncio
from typing import Optional

from shared.consumers.command_consumer_base import BaseCommandConsumer
from shared.models.common import CommandMessage
from client import MQTTClientWrapper
import config

logger = logging.getLogger(__name__)

class CommandConsumer(BaseCommandConsumer):
    """
    MQTT command consumer that consumes device command messages from Kafka and publishes them to MQTT topics.
    Uses AsyncResilientKafkaConsumer for automatic error handling and reconnection.
    """
    
    def __init__(self, mqtt_client: MQTTClientWrapper):
        """
        Initialize the MQTT command consumer.
        
        Args:
            mqtt_client: The MQTT client wrapper to use for publishing
        """
        super().__init__(
            consumer_group_id="iot_command_consumer_mqtt",
            protocol="mqtt",
            kafka_topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
            kafka_bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
        self.mqtt_client = mqtt_client

    def should_process_command(self, command_message: CommandMessage) -> bool:
        """Check if this is an MQTT command."""
        return command_message.protocol.lower() == 'mqtt'

    async def handle_protocol_specific_processing(self, command_message: CommandMessage, formatted_data: dict) -> bool:
        """
        MQTT-specific processing: publish to MQTT topic.
        
        Args:
            command_message: Original CommandMessage
            formatted_data: Formatted command dict
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            request_id = command_message.request_id
            device_id = command_message.device_id
            logger.info(f"formatted_data; {formatted_data}")
            metadata = formatted_data.get('metadata', {})
            # Extract topic from metadata (required for MQTT)
            topic = metadata.get('topic', None)
            if not topic:
                logger.error(f"[{request_id}] MQTT command missing topic in metadata for device {device_id}")
                return False
            
            # Extract optional MQTT-specific fields from metadata
            qos = metadata.get('qos', None)
            retain = metadata.get('retain', None)
            
            logger.info(f"[{request_id}] Processing command for device {device_id} on topic {topic}")
            
            # Publish the message to MQTT using executor for the synchronous call
            success = await asyncio.get_event_loop().run_in_executor(
                None,
                self.mqtt_client.publish_message,
                topic,
                formatted_data.get('payload', {}),
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
            logger.exception(f"Error in MQTT protocol-specific processing: {e}")
            return False 
    
    def acknowledge_command(self, device_id: str, command_id: str) -> bool:
        """
        Acknowledge that a command has been delivered to a device.
        Removes the command from the pending list.
        
        Args:
            device_id: The device ID the command was for
            command_id: The unique identifier of the command (request_id)
            
        Returns:
            True if the command was found and removed, False otherwise
        """
        if device_id not in self.pending_commands:
            return False
            
        # Find the command by request_id
        for i, command in enumerate(self.pending_commands[device_id]):
            if command.get('request_id') == command_id:
                # Remove it from the list
                self.pending_commands[device_id].pop(i)
                return True
                
        return False
