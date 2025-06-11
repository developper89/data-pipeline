# coap_gateway/server.py
import logging
import asyncio

import aiocoap
import aiocoap.resource as resource

from resources import DataRootResource
from kafka_producer import KafkaMsgProducer # Import Kafka producer wrapper
from command_consumer import CommandConsumer
import config

logger = logging.getLogger(__name__)

class CoapGatewayServer:
    def __init__(self, host: str, port: int, kafka_producer: KafkaMsgProducer): # Updated type hint
        self.host = host
        self.port = port
        self.kafka_producer = kafka_producer # Store Kafka producer wrapper
        self.protocol = None
        self._run_task = None
        self._stop_event = asyncio.Event()
        self.command_consumer = None
        
    async def start(self):
        """Creates the CoAP context and starts the server."""
        logger.info(f"Starting CoAP Gateway on {self.host}:{self.port}")
        try:
            # Initialize the command consumer
            self.command_consumer = CommandConsumer()
            await self.command_consumer.start()
            logger.info("Command consumer started")
            
            # Create the data resource that will handle both device data and commands
            data_root = DataRootResource(self.kafka_producer, self.command_consumer)
            
            # Create a root site and mount the data resource under the configured path
            # root_site = resource.Site()
            # root_site.add_resource([], data_root)
            # root_site.add_resource(['*'], data_root)
            
            # The aiocoap server context needs the site root
            self.protocol = await aiocoap.Context.create_server_context(
                data_root,  # Pass DataRootResource as the site root
                bind=(self.host, self.port)
            )
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{timestamp}] Registered CoAP endpoint at path: /{'/'.join(config.COAP_BASE_DATA_PATH)}/{{device_id}}")
            logger.info("CoAP server context created successfully.")

            # Keep the server running until stop event is set
            self._run_task = asyncio.create_task(self._wait_for_stop())
            await self._run_task # Wait until stop is called

        except OSError as e:
             logger.error(f"Failed to bind CoAP server to {self.host}:{self.port}. Error: {e}")
             logger.error("Ensure the port is not already in use and the host address is correct.")
             raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred during CoAP server startup: {e}")
            raise

    async def _wait_for_stop(self):
        """Coroutine that waits for the stop event."""
        await self._stop_event.wait()
        logger.info("Stop event received, shutting down CoAP context.")

    async def stop(self):
        """Stops the CoAP server gracefully."""
        if self._stop_event.is_set():
             logger.debug("Stop already called.")
             return

        logger.info("Stopping CoAP Gateway Server...")
        self._stop_event.set() # Signal the run loop to stop

        # Stop the command consumer
        if self.command_consumer:
            logger.info("Stopping command consumer...")
            await self.command_consumer.stop()
            self.command_consumer = None
            logger.info("Command consumer stopped")

        # Allow the run task to exit cleanly if possible
        if self._run_task and not self._run_task.done():
             try:
                 await asyncio.wait_for(self._run_task, timeout=2.0)
             except asyncio.TimeoutError:
                 logger.warning("CoAP server run task did not finish promptly.")
                 self._run_task.cancel() # Force cancellation if timeout exceeded
                 try:
                     await self._run_task # Await cancellation
                 except asyncio.CancelledError:
                      logger.debug("Run task cancelled.")
             except Exception as e:
                 logger.error(f"Error waiting for CoAP run task: {e}")

        # Shutdown aiocoap context
        if self.protocol:
            logger.info("Shutting down aiocoap context...")
            await self.protocol.shutdown()
            self.protocol = None
            logger.info("aiocoap context shut down.")
        else:
            logger.info("No active aiocoap context to shut down.")