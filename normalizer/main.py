# normalizer_service/main.py
import asyncio
import logging
import signal
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from shared.db.database import init_db
from preservarium_sdk.infrastructure.sql_repository.sql_parser_repository import SQLParserRepository
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import SQLDatatypeRepository

from preservarium_sdk.infrastructure.sql_repository.sql_sensor_repository import SQLSensorRepository
from preservarium_sdk.infrastructure.sql_repository.sql_hardware_repository import SQLHardwareRepository
from sqlalchemy.ext.asyncio import AsyncSession

# Ensure other local modules are importable if running as main
# sys.path.append(os.path.dirname(__file__)) # Or use `python -m normalizer_service.main`

import config
from service import NormalizerService

log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.DEBUG)
# Configure logging
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress overly verbose library logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO if os.getenv("SQL_DEBUG") else logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("parser_script").setLevel(log_level)
# logging.getLogger("parser_script").setLevel(logging.DEBUG)

# Create a custom filter to add request_id to log records when available
class RequestIdFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = '-'
        return True

# Add filter to root logger
logger = logging.getLogger()
logger.addFilter(RequestIdFilter())

# Optional: Add logger to enable specific request tracking in your log messages
# Example: logger.info(f"[{request_id}] Processing message")
service_logger = logging.getLogger("normalizer_service")

# Global signal handlers
SHUTDOWN_SIGNAL_RECEIVED = False

async def setup_signal_handlers(service: NormalizerService) -> None:
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
async def manage_database() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database session.
    Handles initial connection and cleanup.
    """
    db_session = None
    try:
        logger.info("Initializing database connection...")
        db_session = await init_db()
        yield db_session
    finally:
        if db_session:
            logger.info("Closing database connection...")
            await db_session.close()

@asynccontextmanager
async def manage_service(db_session: AsyncSession) -> AsyncGenerator[NormalizerService, None]:
    """
    Async context manager for service lifecycle.
    Handles initialization and cleanup of the service.
    """
    service = None
    try:
        logger.info("Initializing Normalizer Service...")
        # Initialize repositories
        parser_repository = SQLParserRepository(db_session)
        datatype_repository = SQLDatatypeRepository(db_session)
        sensor_repository = SQLSensorRepository(db_session)
        hardware_repository = SQLHardwareRepository(db_session)
        # Create service with all repositories for enhanced validation
        service = NormalizerService(
            parser_repository=parser_repository,
            datatype_repository=datatype_repository,
            sensor_repository=sensor_repository,
            hardware_repository=hardware_repository
        )
        
        yield service
    finally:
        if service:
            logger.info("Stopping Normalizer Service...")
            await service.stop()

async def run_service() -> None:
    """
    Main service runner that manages the lifecycle of all components.
    Uses async context managers to ensure proper cleanup.
    """
    try:
        async with manage_database() as db_session:
            async with manage_service(db_session) as service:
                await setup_signal_handlers(service)
                logger.info("Starting Normalizer Service...")
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
        logger.info("Normalizer service shutdown complete.")