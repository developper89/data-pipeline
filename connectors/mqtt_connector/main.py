# mqtt_connector/main.py
import logging
import signal
import sys
import time

from kafka_producer import KafkaMsgProducer
from shared.mq.kafka_helpers import create_kafka_producer
from client import MQTTClientWrapper
import config

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Suppress overly verbose library logs if desired
# logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

kafka_producer_instance = None
mqtt_client_wrapper = None

def main():
    global kafka_producer_instance, mqtt_client_wrapper

    logger.info("Starting MQTT Connector Service...")

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Initialize Kafka Producer
        kafka_producer_instance = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)
        if not kafka_producer_instance:
            raise RuntimeError("Failed to initialize Kafka Producer.")

        kafka_msg_prod = KafkaMsgProducer(kafka_producer_instance)
        # Initialize MQTT Client
        mqtt_client_wrapper = MQTTClientWrapper(kafka_msg_prod)
        mqtt_client_wrapper.connect()  # Initiate connection

        # Start MQTT blocking loop (handles reconnects)
        mqtt_client_wrapper.start_loop()

    except Exception as e:
        logger.exception(f"Fatal error during MQTT Connector execution: {e}")
        sys.exit(1)
    finally:
        # Cleanup is handled by signal_handler or upon loop exit
        logger.info("MQTT Connector main function finished.")
        cleanup()

def signal_handler(signum, frame):
    """Handles shutdown signals."""
    logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
    cleanup()
    sys.exit(0)

def cleanup():
    """Perform graceful shutdown."""
    global kafka_producer_instance, mqtt_client_wrapper
    logger.info("Starting cleanup...")
    
    if mqtt_client_wrapper:
        logger.info("Stopping MQTT client...")
        try:
            mqtt_client_wrapper.stop_loop()
        except Exception as e:
            logger.error(f"Error stopping MQTT client: {e}")
        mqtt_client_wrapper = None  # Allow garbage collection

    if kafka_producer_instance:
        logger.info("Closing Kafka producer...")
        try:
            kafka_producer_instance.close()
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        kafka_producer_instance = None

    logger.info("Cleanup complete.")

if __name__ == "__main__":
    main()