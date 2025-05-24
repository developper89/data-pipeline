# preservarium_pusher/kafka/kafka_producer.py
import logging
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import shared components
from shared.mq.kafka_helpers import publish_message

logger = logging.getLogger(__name__)

class PusherKafkaProducer:
    """
    Wrapper around KafkaProducer for publishing device commands.
    Handles publishing to the appropriate Kafka topics for device commands.
    """
    def __init__(self, producer: KafkaProducer, command_topic: str):
        """
        Initialize with a pre-configured KafkaProducer instance.
        
        Args:
            producer: A pre-configured KafkaProducer instance
            command_topic: The Kafka topic to publish device commands to
        """
        if not isinstance(producer, KafkaProducer):
            raise TypeError("Producer must be a KafkaProducer instance")
            
        self.producer = producer
        self.command_topic = command_topic
        logger.info(f"PusherKafkaProducer initialized with command topic '{command_topic}'")

    def publish_command(self, command):
        """
        Publish a command message to the command topic.
        
        Args:
            command: The CommandMessage object to publish
            
        Raises:
            KafkaError: If there's a Kafka-related error during publishing
            Exception: For other unexpected errors
        """
        if not hasattr(command, 'device_id') or not hasattr(command, 'request_id'):
            raise TypeError("Command must have device_id and request_id attributes")

        try:
            # Use device_id as the key for partitioning
            key = command.device_id
            
            # Convert to dict if it's a model object
            if hasattr(command, 'model_dump'):
                value = command.model_dump()
            elif hasattr(command, 'dict'):
                value = command.dict()
            else:
                value = command  # Assume it's already a dict
                
            logger.debug(f"[{command.request_id}] Publishing command to topic '{self.command_topic}' with key '{key}'")
            publish_message(self.producer, self.command_topic, value, key)
        except KafkaError as e:
            logger.error(f"[{command.request_id}] Failed to publish command: {e}")
            raise
        except Exception as e:
            logger.exception(f"[{command.request_id}] Unexpected error publishing command: {e}")
            raise
            
    def publish_command_dict(self, command_dict: Dict[str, Any]):
        """
        Publish a command from a dictionary.
        
        Args:
            command_dict: A dictionary with the command data
            
        Raises:
            ValueError: If required fields are missing
            KafkaError: If there's a Kafka-related error during publishing
            Exception: For other unexpected errors
        """
        # Validate required fields
        if 'device_id' not in command_dict:
            raise ValueError("Command dictionary must contain 'device_id'")
            
        # Ensure request_id is present
        if 'request_id' not in command_dict:
            from shared.models.common import generate_request_id
            command_dict['request_id'] = generate_request_id()
            
        # Create a simple object with the required attributes for logging
        class CommandWrapper:
            def __init__(self, cmd_dict):
                self.device_id = cmd_dict['device_id']
                self.request_id = cmd_dict['request_id']
                
        cmd_wrapper = CommandWrapper(command_dict)
        
        try:
            # Use device_id as the key for partitioning
            key = command_dict['device_id']
            
            logger.debug(f"[{cmd_wrapper.request_id}] Publishing command dict to topic '{self.command_topic}' with key '{key}'")
            publish_message(self.producer, self.command_topic, command_dict, key)
        except KafkaError as e:
            logger.error(f"[{cmd_wrapper.request_id}] Failed to publish command dict: {e}")
            raise
        except Exception as e:
            logger.exception(f"[{cmd_wrapper.request_id}] Unexpected error publishing command dict: {e}")
            raise

    def close(self, timeout: Optional[float] = None):
        """
        Close the underlying KafkaProducer.
        
        Args:
            timeout: The maximum time to wait for buffered messages to be delivered
        """
        if self.producer:
            logger.info(f"Closing PusherKafkaProducer (timeout={timeout}s)...")
            try:
                self.producer.close(timeout=timeout)
                logger.info("Producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
            finally:
                self.producer = None 