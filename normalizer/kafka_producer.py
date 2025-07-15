# normalizer_service/kafka_producer.py
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaError, KafkaException

# Import shared components
from shared.mq.kafka_helpers import publish_message, create_kafka_producer
from shared.models.common import ErrorMessage, ValidatedOutput, AlertMessage, AlarmMessage

# Import local config
import config

logger = logging.getLogger(__name__)

class NormalizerKafkaProducer:
    """
    A wrapper around confluent-kafka Producer specifically for the Normalizer Service.
    Handles publishing ValidatedOutput and ErrorMessage to the correct topics.
    """
    def __init__(self, producer: Producer):
        """
        Initializes the wrapper with a pre-configured Producer instance.
        """
        if not isinstance(producer, Producer):
            raise TypeError("Producer must be an instance of confluent_kafka.Producer")
        self.producer = producer
        logger.info("NormalizerKafkaProducer initialized.")

    @classmethod
    def create(cls, bootstrap_servers: str, **config_overrides):
        """
        Create a NormalizerKafkaProducer with a new Producer instance.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            **config_overrides: Additional producer configuration
            
        Returns:
            NormalizerKafkaProducer instance
        """
        producer = create_kafka_producer(bootstrap_servers, **config_overrides)
        return cls(producer)

    def publish_validated_data(self, message: ValidatedOutput):
        """
        Publishes a ValidatedOutput message to the configured validated data topic.

        Args:
            message: The ValidatedOutput object to publish.

        Raises:
            KafkaException: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, ValidatedOutput):
            raise TypeError("Message must be an instance of ValidatedOutput")

        try:
            # Use device_id as the key for partitioning validated data
            key = message.device_id
            topic = config.KAFKA_VALIDATED_DATA_TOPIC
            value = message.model_dump()  # Convert Pydantic model to dict

            logger.debug(f"Publishing validated data for device {message.device_id} (req: {message.request_id}) to topic '{topic}'")
            publish_message(self.producer, topic, value, key)

        except KafkaException as e:
            logger.error(f"[{message.request_id}] Failed to publish ValidatedOutput to Kafka topic '{topic}': {e}")
            raise
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing ValidatedOutput to Kafka topic '{topic}': {e}")
            raise

    def publish_error(self, message: ErrorMessage):
        """
        Publishes an ErrorMessage to the configured error topic.

        Args:
            message: The ErrorMessage object to publish.

        Raises:
            KafkaException: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, ErrorMessage):
            raise TypeError("Message must be an instance of ErrorMessage")

        if not config.KAFKA_ERROR_TOPIC:
            logger.warning(f"[{message.request_id}] KAFKA_ERROR_TOPIC not configured. Skipping error publication.")
            return

        try:
            key = message.request_id
            topic = config.KAFKA_ERROR_TOPIC
            value = message.model_dump()

            logger.debug(f"[{message.request_id}] Publishing ErrorMessage to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)

        except KafkaException as e:
            logger.error(f"[{message.request_id}] Failed to publish ErrorMessage to Kafka topic '{topic}': {e}")
            raise
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing ErrorMessage to Kafka topic '{topic}': {e}")
            raise

    def publish_alert(self, message: AlertMessage):
        """
        Publishes an AlertMessage to the configured alert topic.

        Args:
            message: The AlertMessage object to publish.

        Raises:
            KafkaException: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, AlertMessage):
            raise TypeError("Message must be an instance of AlertMessage")

        if not config.KAFKA_ALERT_TOPIC:
            logger.warning(f"[{message.request_id}] KAFKA_ALERT_TOPIC not configured. Skipping alert publication.")
            return

        try:
            key = message.device_id
            topic = config.KAFKA_ALERT_TOPIC
            value = message.model_dump()

            logger.debug(f"[{message.request_id}] Publishing AlertMessage to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)

        except KafkaException as e:
            logger.error(f"[{message.request_id}] Failed to publish AlertMessage to Kafka topic '{topic}': {e}")
            raise
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing AlertMessage to Kafka topic '{topic}': {e}")
            raise

    def publish_alarm(self, message: AlarmMessage):
        """
        Publishes an AlarmMessage to the configured alarm topic.

        Args:
            message: The AlarmMessage object to publish.

        Raises:
            KafkaException: If publishing fails due to Kafka-related issues.
            Exception: For other unexpected publishing errors.
        """
        if not isinstance(message, AlarmMessage):
            raise TypeError("Message must be an instance of AlarmMessage")

        if not config.KAFKA_ALARM_TOPIC:
            logger.warning(f"[{message.request_id}] KAFKA_ALARM_TOPIC not configured. Skipping alarm publication.")
            return

        try:
            key = message.device_id
            topic = config.KAFKA_ALARM_TOPIC
            value = message.model_dump()

            logger.debug(f"[{message.request_id}] Publishing AlarmMessage to topic '{topic}' with key '{key}'")
            publish_message(self.producer, topic, value, key)

        except KafkaException as e:
            logger.error(f"[{message.request_id}] Failed to publish AlarmMessage to Kafka topic '{topic}': {e}")
            raise
        except Exception as e:
            logger.exception(f"[{message.request_id}] Unexpected error publishing AlarmMessage to Kafka topic '{topic}': {e}")
            raise

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
            logger.info(f"Closing NormalizerKafkaProducer's underlying Producer (timeout={timeout}s)...")
            try:
                # Flush any remaining messages before closing
                remaining = self.producer.flush(timeout or 30.0)
                if remaining > 0:
                    logger.warning(f"Producer closed with {remaining} messages still in queue")
                
                logger.info("Underlying Producer closed successfully.")
            except Exception as e:
                logger.error(f"Error closing underlying Producer: {e}", exc_info=True)
            finally:
                self.producer = None
        else:
            logger.warning("Attempted to close NormalizerKafkaProducer, but no active producer found.")