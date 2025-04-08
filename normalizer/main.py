# normalizer_service/main.py
import asyncio
import logging
import signal
import os
import sys
from shared.db.database import init_db
from preservarium_sdk.infrastructure.repository.sql_parser_repository import SQLParserRepository

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

service_instance = None

async def main():
    global service_instance
    logger.info("Initializing Normalizer Service...")
    
    # Initialize database session
    db_session = await init_db()
    
    try:
        # Create parser repository with SQLAlchemy session
        parser_repository = SQLParserRepository(db_session)
        
        # Create the service instance
        service_instance = NormalizerService(parser_repository)

        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(shutdown(s, loop, service_instance))
            )

        # Run the service's main loop
        await service_instance.run()
    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        raise
    finally:
        # Ensure we close the database session
        await db_session.close()
        logger.info("Database session closed")

async def shutdown(signal, loop, service: NormalizerService):
    """Initiates graceful shutdown."""
    logger.warning(f"Received exit signal {signal.name}... Initiating shutdown...")

    if service:
        # Signal the service to stop its loops
        await service.stop()

    logger.info("Shutdown sequence initiated. Allowing run loop to finish cleanup...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Normalizer service stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.exception("Normalizer service stopped due to unexpected error at top level.", exc_info=e)
        sys.exit(1) # Exit with error code
    finally:
        logger.info("Normalizer service process finished.")