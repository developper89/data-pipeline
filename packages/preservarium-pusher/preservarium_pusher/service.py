# preservarium_pusher/service.py
import logging
import asyncio
from typing import Dict, Any, Optional, Union

from kafka import KafkaProducer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import create_kafka_producer
from preservarium_pusher.core.models import CommandMessage, DeviceCommand, DeviceSettings
from preservarium_pusher.kafka.kafka_producer import PusherKafkaProducer

logger = logging.getLogger(__name__)

class PusherService:
    """
    Main service class for pushing commands and settings to IoT devices.
    Uses Kafka to publish messages that will be consumed by connectors.
    """
    
    def __init__(self, kafka_bootstrap_servers: str, command_topic: str):
        """
        Initialize the Pusher Service.
        
        Args:
            kafka_bootstrap_servers: Comma-separated list of Kafka bootstrap servers
            command_topic: The Kafka topic for publishing device commands
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.command_topic = command_topic
        self.kafka_producer = None
        self._producer_wrapper = None
        self._stop_event = asyncio.Event()
        
    async def initialize(self) -> bool:
        """
        Initialize the service by connecting to Kafka.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            logger.info("Initializing PusherService...")
            # Create the Kafka producer
            raw_producer = create_kafka_producer(self.kafka_bootstrap_servers)
            
            if not raw_producer:
                logger.error("Failed to create Kafka producer.")
                return False
                
            # Create the producer wrapper
            self._producer_wrapper = PusherKafkaProducer(
                producer=raw_producer,
                command_topic=self.command_topic
            )
            
            logger.info("PusherService initialized successfully.")
            return True
        except Exception as e:
            logger.exception(f"Error initializing PusherService: {e}")
            await self.close()
            return False
    
    async def push_command(self, command: CommandMessage) -> bool:
        """
        Push a command to a device through Kafka.
        
        Args:
            command: The CommandMessage object to push
            
        Returns:
            bool: True if the command was published successfully, False otherwise
        """
        if not self._producer_wrapper:
            logger.error("Cannot push command: Kafka producer not initialized.")
            return False
            
        try:
            self._producer_wrapper.publish_command(command)
            return True
        except Exception as e:
            logger.error(f"[{command.request_id}] Error pushing command: {e}")
            return False
    
    async def push_command_dict(self, command_dict: Dict[str, Any]) -> bool:
        """
        Push a command from a dictionary.
        
        Args:
            command_dict: Dictionary containing command data
            
        Returns:
            bool: True if the command was published successfully, False otherwise
        """
        if not self._producer_wrapper:
            logger.error("Cannot push command: Kafka producer not initialized.")
            return False
            
        try:
            self._producer_wrapper.publish_command_dict(command_dict)
            return True
        except Exception as e:
            request_id = command_dict.get('request_id', 'unknown')
            logger.error(f"[{request_id}] Error pushing command dict: {e}")
            return False
    
    async def create_and_push_command(self, 
                                    device_id: str, 
                                    command_type: str,
                                    payload: Dict[str, Any],
                                    protocol: str,
                                    parser_script_ref: Optional[str] = None,
                                    metadata: Optional[Dict[str, Any]] = None,
                                    **kwargs) -> bool:
        """
        Create a command from parameters and push it.
        
        Args:
            device_id: The device ID to send the command to
            command_type: Type of command
            payload: The command payload data
            protocol: The protocol to use (mqtt or coap)
            parser_script_ref: Optional parser script reference for command formatting (e.g., "efento_bidirectional_parser.py")
            metadata: Optional protocol-specific metadata (e.g., {"topic": "devices/123/commands"} for MQTT)
            **kwargs: Additional command parameters
            
        Returns:
            bool: True if the command was published successfully, False otherwise
        """
        try:
            # Create the command object
            command = CommandMessage(
                device_id=device_id,
                command_type=command_type,
                payload=payload,
                protocol=protocol,
                parser_script_ref=parser_script_ref,
                metadata=metadata,
                **kwargs
            )
            
            # Push the command
            return await self.push_command(command)
        except Exception as e:
            logger.error(f"Error creating and pushing command: {e}")
            return False
    
    async def close(self, timeout: Optional[float] = None) -> None:
        """
        Close the service and its resources.
        
        Args:
            timeout: Optional timeout for closing the Kafka producer
        """
        logger.info("Closing PusherService...")
        
        # Close the Kafka producer wrapper if it exists
        if self._producer_wrapper:
            try:
                self._producer_wrapper.close(timeout=timeout)
            except Exception as e:
                logger.error(f"Error closing Kafka producer wrapper: {e}")
            finally:
                self._producer_wrapper = None
                
        logger.info("PusherService closed.")
        
    async def stop(self) -> None:
        """Signal the service to stop and close resources."""
        if not self._stop_event.is_set():
            logger.info("Stopping PusherService...")
            self._stop_event.set()
            await self.close() 