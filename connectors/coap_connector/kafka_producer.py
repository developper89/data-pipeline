# coap_gateway/kafka_producer.py
import logging
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import shared components
from shared.mq.kafka_helpers import publish_message # Use the generic helper
from shared.models.common import RawMessage, ErrorMessage

# Import local config
import config

logger = logging.getLogger(__name__)

class KafkaMsgProducer:
    """
    A wrapper around KafkaProducer specifically for the CoAP Gateway Service.
    Handles publishing RawMessage and ErrorMessage to the correct Kafka topics.
    """
    def __init__(self, producer: KafkaProducer):
        """
        Initializes the wrapper with a pre-configured KafkaProducer instance.
        """
        if not isinstance(producer, KafkaProducer):
            raise TypeError("Producer must be an instance of kafka.KafkaProducer")
        self.producer = producer
        logger.info("CoAP Gateway KafkaMsgProducer initialized.")

    def publish_raw_message(self, message: RawMessage):
        """
        Publishes a RawMessage to the configured raw data topic.

        Args:
            message: The RawMessage object to publish.

        Raises:
            KafkaError: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, RawMessage):
            raise TypeError("Message must be an instance of RawMessage")

        try:
            # Use device_id as the key for partitioning raw data
            key = message.device_id
            topic = config.KAFKA_RAW_DATA_TOPIC
            value = message.model_dump() # Convert Pydantic model to dict

            logger.debug(f"[{message.request_id}] Publishing RawMessage to topic '{topic}' with key '{key}'")
            # Use the synchronous helper function
            publish_message(self.producer, topic, value, key)
            # Logging of success/failure happens within publish_message helper

        except KafkaError as e:
            logger.error(f"[{message.request_id}] Failed to publish RawMessage to Kafka topic '{topic}': {e}")
            raise # Re-raise Kafka specific errors for CoAP handler to react
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing RawMessage to Kafka topic '{topic}': {e}")
            raise # Re-raise other errors

    def publish_error(self, error_type: str, error: str, details: Optional[dict] = None, request_id: Optional[str] = None):
        """
        Publishes a gateway ErrorMessage to the configured error topic.

        Args:
            error_type: A short description of the error type.
            error: The detailed error message.
            details: Optional dictionary with additional context.
            request_id: Optional request ID associated with the error.

        Raises:
            KafkaError: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not config.KAFKA_ERROR_TOPIC:
            logger.warning(f"[{request_id}] KAFKA_ERROR_TOPIC not configured. Skipping gateway error publication.")
            return # Don't raise an error, just skip if not configured

        try:
            # Create the ErrorMessage object
            error_msg = ErrorMessage(
                request_id=request_id, # Include if available
                service=config.SERVICE_NAME,
                error=f"{error_type}: {error}",
                details=details or {}
            )

            key = request_id # Use request_id as key if available
            topic = config.KAFKA_ERROR_TOPIC
            value = error_msg.model_dump()

            logger.debug(f"[{request_id}] Publishing Gateway ErrorMessage to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)

        except KafkaError as e:
            logger.error(f"[{request_id}] Failed to publish Gateway ErrorMessage to Kafka topic '{topic}': {e}")
            raise # Re-raise Kafka specific errors
        except Exception as e:
            logger.exception(f"[{request_id}] Unexpected error publishing Gateway ErrorMessage to Kafka topic '{topic}': {e}")
            raise # Re-raise other errors

    def close(self, timeout: Optional[float] = None):
        """
        Closes the underlying KafkaProducer.

        Args:
            timeout: The maximum time to wait for buffered messages to be sent.
        """
        if self.producer:
            logger.info(f"Closing CoAP Gateway KafkaMsgProducer's underlying KafkaProducer (timeout={timeout}s)...")
            try:
                self.producer.close(timeout=timeout)
                logger.info("Underlying KafkaProducer closed successfully.")
            except Exception as e:
                 logger.error(f"Error closing underlying KafkaProducer: {e}", exc_info=True)
            finally:
                 self.producer = None
        else:
            logger.warning("Attempted to close CoAP Gateway KafkaMsgProducer, but no active producer found.")