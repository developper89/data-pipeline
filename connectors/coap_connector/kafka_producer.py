# coap_gateway/kafka_producer.py
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaError, KafkaException

# Import shared components
from shared.mq.kafka_helpers import publish_message, create_kafka_producer
from shared.models.common import RawMessage, ErrorMessage

# Import local config
import config

logger = logging.getLogger(__name__)

class KafkaMsgProducer:
    """
    A wrapper around confluent-kafka Producer specifically for the CoAP Gateway Service.
    Handles publishing RawMessage and ErrorMessage to the correct Kafka topics.
    """
    def __init__(self, producer: Producer):
        """
        Initializes the wrapper with a pre-configured Producer instance.
        """
        if not isinstance(producer, Producer):
            raise TypeError("Producer must be an instance of confluent_kafka.Producer")
        self.producer = producer
        logger.info("CoAP Gateway KafkaMsgProducer initialized.")

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

    def publish_raw_message(self, message: RawMessage):
        """
        Publishes a RawMessage to the configured raw data topic.

        Args:
            message: The RawMessage object to publish.

        Raises:
            KafkaException: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        try:
            # Use device_id as the key for partitioning raw data
            key = message.device_id
            topic = config.KAFKA_RAW_DATA_TOPIC
            value = message.model_dump()  # Convert Pydantic model to dict

            logger.debug(f"Published raw message for device {message.device_id} (req: {message.request_id}) to Kafka topic '{config.KAFKA_RAW_DATA_TOPIC}' with payload: {message.payload_hex}")
            # Use the synchronous helper function
            publish_message(self.producer, topic, value, key)
            # Logging of success/failure happens within publish_message helper

        except KafkaException as e:
            logger.error(f"[{message.request_id}] Failed to publish RawMessage to Kafka topic '{topic}': {e}")
            raise  # Re-raise Kafka specific errors for CoAP handler to react
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing RawMessage to Kafka topic '{topic}': {e}")
            raise  # Re-raise other errors

    def publish_error(self, error_type: str, error: str, details: Optional[dict] = None, request_id: Optional[str] = None):
        """
        Publishes a gateway ErrorMessage to the configured error topic.

        Args:
            error_type: A short description of the error type.
            error: The detailed error message.
            details: Optional dictionary with additional context.
            request_id: Optional request ID associated with the error.

        Raises:
            KafkaException: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not config.KAFKA_ERROR_TOPIC:
            logger.warning(f"[{request_id}] KAFKA_ERROR_TOPIC not configured. Skipping gateway error publication.")
            return  # Don't raise an error, just skip if not configured

        try:
            # Create the ErrorMessage object
            error_msg = ErrorMessage(
                request_id=request_id,  # Include if available
                service=config.SERVICE_NAME,
                error=f"{error_type}: {error}",
                details=details or {}
            )

            key = request_id  # Use request_id as key if available
            topic = config.KAFKA_ERROR_TOPIC
            value = error_msg.model_dump()

            logger.debug(f"[{request_id}] Publishing Gateway ErrorMessage to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)

        except KafkaException as e:
            logger.error(f"[{request_id}] Failed to publish Gateway ErrorMessage to Kafka topic '{topic}': {e}")
            raise  # Re-raise Kafka specific errors
        except Exception as e:
            logger.exception(f"[{request_id}] Unexpected error publishing Gateway ErrorMessage to Kafka topic '{topic}': {e}")
            raise  # Re-raise other errors

    def flush(self, timeout: Optional[float] = None):
        """
        Wait for all messages in the Producer queue to be delivered.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Number of messages still in queue
        """
        if self.producer:
            try:
                remaining = self.producer.flush(timeout or 30.0)
                if remaining > 0:
                    logger.warning(f"Producer flush completed with {remaining} messages still in queue")
                else:
                    logger.debug("Producer flush completed successfully")
                return remaining
            except Exception as e:
                logger.error(f"Error during producer flush: {e}")
                return -1
        return 0

    def close(self, timeout: Optional[float] = None):
        """
        Closes the underlying Producer.

        Args:
            timeout: The maximum time to wait for buffered messages to be sent.
        """
        if self.producer:
            logger.info(f"Closing CoAP Gateway KafkaMsgProducer's underlying Producer (timeout={timeout}s)...")
            try:
                # Flush any remaining messages before closing
                remaining = self.producer.flush(timeout or 30.0)
                if remaining > 0:
                    logger.warning(f"Producer closed with {remaining} messages still in queue")
                
                # confluent-kafka Producer doesn't have an explicit close() method
                # Setting to None will allow garbage collection
                logger.info("Underlying Producer closed successfully.")
            except Exception as e:
                logger.error(f"Error closing underlying Producer: {e}", exc_info=True)
            finally:
                self.producer = None
        else:
            logger.warning("Attempted to close CoAP Gateway KafkaMsgProducer, but no active producer found.")