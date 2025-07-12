# mqtt_connector/main.py
import logging
import signal
import sys
import asyncio
import threading
import os

from kafka_producer import KafkaMsgProducer
from shared.mq.kafka_helpers import create_kafka_producer
from client import MQTTClientWrapper
from command_consumer import CommandConsumer
import config

# Configure logging
log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.DEBUG)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logging.getLogger("kafka").setLevel(logging.WARNING)

logging.getLogger("client").setLevel(log_level)
logging.getLogger("shared.translation").setLevel(log_level)



logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
kafka_producer_instance = None
mqtt_client_wrapper = None
command_consumer = None
shutdown_event = None
mqtt_thread = None

async def main():
    """Main async function."""
    global kafka_producer_instance, mqtt_client_wrapper, command_consumer, shutdown_event, mqtt_thread

    logger.info("Starting Hybrid MQTT Connector Service...")
    
    # Create shutdown event
    shutdown_event = asyncio.Event()
    
    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Initialize Kafka Producer
        kafka_producer_instance = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)
        if not kafka_producer_instance:
            raise RuntimeError("Failed to initialize Kafka Producer.")

        kafka_msg_prod = KafkaMsgProducer(kafka_producer_instance)
        
        # Initialize MQTT Client with event loop reference
        event_loop = asyncio.get_running_loop()
        mqtt_client_wrapper = MQTTClientWrapper(kafka_msg_prod, event_loop)
        mqtt_client_wrapper.connect()  # Initiate connection
        
        # Start MQTT client in a separate thread
        mqtt_thread = threading.Thread(target=mqtt_client_wrapper.start_loop, daemon=True)
        mqtt_thread.start()
        logger.info("MQTT client started in background thread")
        
        # Initialize and start the async command consumer
        command_consumer = CommandConsumer(mqtt_client_wrapper)
        await command_consumer.start()
        logger.info("Async command consumer started")

        # Wait for shutdown signal
        await shutdown_event.wait()
        
        logger.info("Shutdown event received, stopping services...")

    except Exception as e:
        logger.exception(f"Fatal error during MQTT Connector execution: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        await cleanup()

async def cleanup():
    """Perform graceful async cleanup."""
    global kafka_producer_instance, mqtt_client_wrapper, command_consumer, mqtt_thread
    logger.info("Starting async cleanup...")
    
    # Stop the command consumer first
    if command_consumer:
        logger.info("Stopping async command consumer...")
        try:
            await command_consumer.stop()
        except Exception as e:
            logger.error(f"Error stopping command consumer: {e}")
        command_consumer = None
    
    if mqtt_client_wrapper:
        logger.info("Stopping MQTT client...")
        try:
            mqtt_client_wrapper.stop_loop()
        except Exception as e:
            logger.error(f"Error stopping MQTT client: {e}")
        mqtt_client_wrapper = None
    
    # Wait for MQTT thread to finish
    if mqtt_thread and mqtt_thread.is_alive():
        logger.info("Waiting for MQTT thread to finish...")
        mqtt_thread.join(timeout=5.0)
        if mqtt_thread.is_alive():
            logger.warning("MQTT thread did not finish within timeout")

    if kafka_producer_instance:
        logger.info("Closing Kafka producer...")
        try:
            # Run the synchronous close in an executor
            await asyncio.get_event_loop().run_in_executor(
                None, kafka_producer_instance.close
            )
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        kafka_producer_instance = None

    logger.info("Async cleanup complete.")

def run_async_main():
    """Run the async main function."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Error in async main: {e}")
    finally:
        logger.info("MQTT Connector service finished.")

if __name__ == "__main__":
    run_async_main()