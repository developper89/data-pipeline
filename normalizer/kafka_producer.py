# normalizer_service/kafka_producer.py
import logging
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import shared components
from shared.mq.kafka_helpers import publish_message # Use the generic helper
from shared.models.common import StandardizedOutput, ErrorMessage

# Import local config
from . import config

logger = logging.getLogger(__name__)

class NormalizerKafkaProducer:
    """
    A wrapper around KafkaProducer specifically for the Normalizer Service.
    Handles publishing StandardizedOutput and ErrorMessage to the correct topics.
    """
    def __init__(self, producer: KafkaProducer):
        """
        Initializes the wrapper with a pre-configured KafkaProducer instance.
        """
        if not isinstance(producer, KafkaProducer):
            raise TypeError("Producer must be an instance of kafka.KafkaProducer")
        self.producer = producer
        logger.info("NormalizerKafkaProducer initialized.")

    def publish_standardized_data(self, message: StandardizedOutput):
        """
        Publishes a StandardizedOutput message to the configured standardized data topic.

        Args:
            message: The StandardizedOutput object to publish.

        Raises:
            KafkaError: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, StandardizedOutput):
            raise TypeError("Message must be an instance of StandardizedOutput")

        try:
            # Use device_id as the key for partitioning standardized data
            key = message.device_id
            topic = config.KAFKA_STANDARDIZED_DATA_TOPIC
            value = message.model_dump() # Convert Pydantic model to dict

            logger.debug(f"[{message.request_id}] Publishing StandardizedOutput to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)
            # publish_message helper already logs success/failure at debug/error level
        except KafkaError as e:
            logger.error(f"[{message.request_id}] Failed to publish StandardizedOutput to Kafka topic '{topic}': {e}")
            raise # Re-raise Kafka specific errors for caller to handle
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing StandardizedOutput to Kafka topic '{topic}': {e}")
            raise # Re-raise other errors

    def publish_error(self, message: ErrorMessage):
        """
        Publishes an ErrorMessage to the configured error topic.

        Args:
            message: The ErrorMessage object to publish.

        Raises:
            KafkaError: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, ErrorMessage):
            raise TypeError("Message must be an instance of ErrorMessage")

        if not config.KAFKA_ERROR_TOPIC:
            logger.warning(f"[{message.request_id}] KAFKA_ERROR_TOPIC not configured. Skipping error publication.")
            return # Don't raise an error, just skip if not configured

        try:
            # Use request_id as the key for partitioning errors, if available
            key = message.request_id
            topic = config.KAFKA_ERROR_TOPIC
            value = message.model_dump()

            logger.debug(f"[{message.request_id}] Publishing ErrorMessage to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)
        except KafkaError as e:
            logger.error(f"[{message.request_id}] Failed to publish ErrorMessage to Kafka topic '{topic}': {e}")
            raise # Re-raise Kafka specific errors
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing ErrorMessage to Kafka topic '{topic}': {e}")
            raise # Re-raise other errors

    def close(self, timeout: Optional[float] = None):
        """
        Closes the underlying KafkaProducer.

        Args:
            timeout: The maximum time to wait for buffered messages to be sent.
        """
        if self.producer:
            logger.info(f"Closing NormalizerKafkaProducer's underlying KafkaProducer (timeout={timeout}s)...")
            try:
                self.producer.close(timeout=timeout)
                logger.info("Underlying KafkaProducer closed successfully.")
            except Exception as e:
                 logger.error(f"Error closing underlying KafkaProducer: {e}", exc_info=True)
            finally:
                 # Avoid reusing a closed producer instance
                 self.producer = None
        else:
            logger.warning("Attempted to close NormalizerKafkaProducer, but no active producer found.")