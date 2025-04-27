import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import config
from service import IngestorService

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress overly verbose library logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("influxdb_client").setLevel(logging.WARNING)

# Create service logger
logger = logging.getLogger("ingestor")

# Global signal handlers
SHUTDOWN_SIGNAL_RECEIVED = False

async def setup_signal_handlers(service: IngestorService) -> None:
    """Setup handlers for OS signals to handle graceful shutdown."""
    def handle_signal(sig, frame):
        global SHUTDOWN_SIGNAL_RECEIVED
        if SHUTDOWN_SIGNAL_RECEIVED:
            logger.warning("Second shutdown signal received, forcing exit")
            sys.exit(1)
        
        SHUTDOWN_SIGNAL_RECEIVED = True
        logger.info(f"Received shutdown signal {sig}, initiating graceful shutdown")
        # Schedule the service to stop in the event loop
        asyncio.create_task(service.stop())
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

@asynccontextmanager
async def manage_service() -> AsyncGenerator[IngestorService, None]:
    """
    Async context manager for service lifecycle.
    Handles initialization and cleanup of the service.
    """
    service = None
    try:
        logger.info("Initializing InfluxDB Ingestor Service")
        service = IngestorService()
        
        # Initialize service
        if not await service.initialize():
            logger.error("Failed to initialize service")
            yield None
            return
            
        yield service
    finally:
        if service:
            logger.info("Stopping InfluxDB Ingestor Service")
            await service.stop()

async def run_service() -> None:
    """
    Main service runner that manages the lifecycle of all components.
    Uses async context managers to ensure proper cleanup.
    """
    try:
        async with manage_service() as service:
            if service is None:
                logger.error("Service initialization failed, exiting")
                return
                
            await setup_signal_handlers(service)
            logger.info("Starting InfluxDB Ingestor Service")
            await service.run()
    except Exception as e:
        logger.exception("Service stopped due to unexpected error:", exc_info=e)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(run_service())
    except KeyboardInterrupt:
        print("\nExiting due to keyboard interrupt")
    except Exception as e:
        logger.exception("Fatal error in main:", exc_info=e)
        sys.exit(1)
    finally:
        logger.info("InfluxDB Ingestor Service shutdown complete.") 