# mqtt_connector/kafka_producer.py
import logging
import asyncio
from kafka import KafkaProducer
from shared.models.common import RawMessage, ErrorMessage
from shared.mq.kafka_helpers import publish_message
import config

logger = logging.getLogger(__name__)

class KafkaMsgProducer:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer

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
                error_msg.model_dump()
            )
            logger.debug(f"Published connector error message to Kafka topic '{config.KAFKA_ERROR_TOPIC}'")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to publish connector error to Kafka topic '{config.KAFKA_ERROR_TOPIC}'. Original error: {error_type} - {error}")

    def close(self):
        if self.producer:
            logger.info("Closing Kafka producer.")
            self.producer.close(timeout=10)