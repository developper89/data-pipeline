# mqtt_connector/kafka_producer.py
import logging
import asyncio
from confluent_kafka import Producer, KafkaError, KafkaException
from shared.models.common import RawMessage, ErrorMessage
from shared.mq.kafka_helpers import publish_message, create_kafka_producer
import config

logger = logging.getLogger(__name__)

class KafkaMsgProducer:
    def __init__(self, producer: Producer):
        if not isinstance(producer, Producer):
            raise TypeError("Producer must be an instance of confluent_kafka.Producer")
        self.producer = producer

    @classmethod
    def create(cls, bootstrap_servers: str, **config_overrides):
        """
        Create a KafkaMsgProducer with a new Producer instance.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            **config_overrides: Additional producer configuration
            
        Returns:
            KafkaMsgProducer instance
        """
        producer = create_kafka_producer(bootstrap_servers, **config_overrides)
        return cls(producer)

    async def publish_raw_message(self, message: RawMessage):
        """Publishes a RawMessage to the configured Kafka topic."""
        try:
            # Use device_id as Kafka key for potential partitioning by device
            # Run the synchronous publish_message in an executor to make it async-compatible
            await asyncio.get_event_loop().run_in_executor(
                None,
                publish_message,
                self.producer,
                config.KAFKA_RAW_DATA_TOPIC,
                message.model_dump(),
                message.device_id
            )
            logger.debug(f"Published raw message for device {message.device_id} (req: {message.request_id}) to Kafka topic '{config.KAFKA_RAW_DATA_TOPIC}' with payload: {message.payload_hex}")
        except Exception as e:
            # Error already logged in publish_message helper
            logger.error(f"Failed to publish raw message for {message.device_id} to Kafka. Error: {e}")
            # Decide if you need to raise or handle this further (e.g., metric)

    async def publish_error(self, error_type: str, error: str, details: dict = None):
        """Publishes a connector error message."""
        if not config.KAFKA_ERROR_TOPIC:
            return
        try:
            error_msg = ErrorMessage(
                service=config.SERVICE_NAME,
                error=f"{error_type}: {error}",
                details=details or {}
            )
            # Errors might not have a specific device key
            # Run the synchronous publish_message in an executor to make it async-compatible
            await asyncio.get_event_loop().run_in_executor(
                None,
                publish_message,
                self.producer,
                config.KAFKA_ERROR_TOPIC,
                error_msg.model_dump(),
                None
            )
            logger.debug(f"Published error message to Kafka topic '{config.KAFKA_ERROR_TOPIC}': {error_type}: {error}")
        except Exception as e:
            logger.error(f"Failed to publish error message to Kafka. Error: {e}")

    async def flush(self, timeout: float = 30.0):
        """
        Wait for all messages in the Producer queue to be delivered.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Number of messages still in queue
        """
        try:
            # Run flush in executor to make it async-compatible
            remaining = await asyncio.get_event_loop().run_in_executor(
                None,
                self.producer.flush,
                timeout
            )
            if remaining > 0:
                logger.warning(f"Producer flush completed with {remaining} messages still in queue")
            else:
                logger.debug("Producer flush completed successfully")
            return remaining
        except Exception as e:
            logger.error(f"Error during producer flush: {e}")
            return -1

    def close(self):
        """Close the producer."""
        if self.producer:
            logger.info("Closing MQTT KafkaMsgProducer...")
            try:
                # Flush any remaining messages before closing
                remaining = self.producer.flush(30.0)
                if remaining > 0:
                    logger.warning(f"Producer closed with {remaining} messages still in queue")
                logger.info("Producer closed successfully.")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
            finally:
                self.producer = None