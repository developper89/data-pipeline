# coap_gateway/main.py
import asyncio
import logging
import signal
import sys
from typing import Optional # For sys.exit

# Shared helpers and local components
from shared.mq.kafka_helpers import create_kafka_producer # For initialization
import config
from kafka_producer import KafkaMsgProducer # The wrapper
from server import CoapGatewayServer

# Configure logging
logging.basicConfig(
    # level=config.LOG_LEVEL,
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# logging.getLogger("aiocoap").setLevel(config.LOG_LEVEL) # aiocoap can be verbose
# logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Global references for cleanup
kafka_producer_wrapper: Optional[KafkaMsgProducer] = None
server_instance: Optional[CoapGatewayServer] = None

async def main():
    global kafka_producer_wrapper, server_instance
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event() # Used to signal shutdown completion

    logger.info("Initializing CoAP Gateway...")

    raw_kafka_producer = None # Keep track of the raw producer for potential cleanup
    try:
        # 1. Initialize Kafka Producer (with retry logic inside helper)
        logger.info(f"Connecting Kafka Producer to {config.KAFKA_BOOTSTRAP_SERVERS}...")
        raw_kafka_producer = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)
        if not raw_kafka_producer:
             raise RuntimeError("Failed to initialize Kafka Producer after retries.")
        logger.info("Kafka Producer connected.")

        # 2. Instantiate the Kafka Producer Wrapper
        kafka_producer_wrapper = KafkaMsgProducer(raw_kafka_producer)

        # 3. Initialize the CoAP server
        server_instance = CoapGatewayServer(
            host=config.COAP_HOST,
            port=config.COAP_PORT,
            kafka_producer=kafka_producer_wrapper # Pass the wrapper
        )

        # 4. Setup signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(shutdown(s, stop_event, server_instance, kafka_producer_wrapper))
            )
        logger.info("Signal handlers set up.")

        # 5. Start the CoAP server (this blocks until stop is called or error)
        await server_instance.start()

    except Exception as e:
         logger.exception(f"Fatal error during startup or runtime: {e}")
         # Ensure cleanup happens even if startup fails partially
         if not stop_event.is_set():
              await shutdown(signal.SIGTERM, stop_event, server_instance, kafka_producer_wrapper)
         sys.exit(1) # Exit with error
    finally:
        logger.info("CoAP Gateway main function exiting.")
        # Cleanup should have been triggered by shutdown signal or exception handler


async def shutdown(sig: signal.Signals, stop_event: asyncio.Event, server: Optional[CoapGatewayServer], kafka_prod: Optional[KafkaMsgProducer]):
    """Cleanup tasks tied to the service's shutdown."""
    if stop_event.is_set():
        logger.info("Shutdown already in progress.")
        return # Avoid duplicate shutdown actions

    logger.warning(f"Received exit signal {sig.name}... Initiating graceful shutdown.")

    # 1. Stop the CoAP server first (releases port, stops accepting new requests)
    if server:
        await server.stop()
    else:
        logger.warning("CoAP server instance not found during shutdown.")

    # 2. Close the Kafka producer (flushes buffer)
    if kafka_prod:
        logger.info("Closing Kafka producer...")
        try:
            # Use the wrapper's close method
            kafka_prod.close(timeout=10)
        except Exception as e:
             logger.error(f"Error closing Kafka producer wrapper: {e}", exc_info=True)
    else:
        logger.warning("Kafka producer wrapper not found during shutdown.")

    # 3. Signal that shutdown is complete
    stop_event.set()
    logger.info("CoAP Gateway shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("CoAP Gateway stopped by user (KeyboardInterrupt).")
    # Ensure final log message after asyncio loop finishes
    logger.info("CoAP Gateway process finished.")