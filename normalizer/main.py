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
from sqlalchemy.ext.asyncio import AsyncSession

# Ensure other local modules are importable if running as main
# sys.path.append(os.path.dirname(__file__)) # Or use `python -m normalizer_service.main`

import config
from service import NormalizerService

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress overly verbose library logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO if os.getenv("SQL_DEBUG") else logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.INFO)

# Create a custom filter to add request_id to log records when available
class RequestIdFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = '-'
        return True

# Create a custom log format handler with filter
root_logger = logging.getLogger()
root_logger.addFilter(RequestIdFilter())

logger = logging.getLogger(__name__)

@asynccontextmanager
async def manage_database() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database session lifecycle.
    Handles initialization and cleanup of database connections.
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
        parser_repository = SQLParserRepository(db_session)
        service = NormalizerService(parser_repository)
        yield service
    finally:
        if service:
            logger.info("Stopping Normalizer Service...")
            await service.stop()

async def setup_signal_handlers(service: NormalizerService) -> None:
    """Setup signal handlers for graceful shutdown."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(handle_shutdown(s, service))
        )

async def handle_shutdown(sig: signal.Signals, service: NormalizerService) -> None:
    """Handle shutdown signal."""
    logger.warning(f"Received exit signal {sig.name}...")
    await service.stop()
    # Get the current event loop and stop it
    loop = asyncio.get_running_loop()
    loop.stop()

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

def main() -> None:
    """Entry point for the service."""
    try:
        asyncio.run(run_service())
    except KeyboardInterrupt:
        logger.info("Service stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.exception("Service stopped due to unexpected error at top level.", exc_info=e)
        sys.exit(1)
    finally:
        logger.info("Service process finished.")

if __name__ == "__main__":
    main()