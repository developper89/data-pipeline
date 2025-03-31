# normalizer_service/main.py
import asyncio
import logging
import signal
import os
import sys

# Ensure other local modules are importable if running as main
# sys.path.append(os.path.dirname(__file__)) # Or use `python -m normalizer_service.main`

from . import config
from .service import NormalizerService

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] %(message)s' # Add request_id to format if available
)
# Filter to add request_id to log records if present in task context (advanced)
# ... (Implementation of Log filter - optional for basic setup)

# Suppress overly verbose library logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO if os.getenv("SQL_DEBUG") else logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.INFO)


logger = logging.getLogger(__name__)

service_instance = None

async def main():
    global service_instance
    logger.info("Initializing Normalizer Service...")

    # Ensure temp dir exists if using subprocess sandbox
    if config.SANDBOX_TYPE == 'subprocess' and config.TEMP_SCRIPT_DIR:
         os.makedirs(config.TEMP_SCRIPT_DIR, exist_ok=True)

    # Create the service instance
    service_instance = NormalizerService()

    # Setup signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(s, loop, service_instance))
        )

    # Run the service's main loop
    await service_instance.run()


async def shutdown(signal, loop, service: NormalizerService):
    """Initiates graceful shutdown."""
    logger.warning(f"Received exit signal {signal.name}... Initiating shutdown...")

    if service:
        # Signal the service to stop its loops
        await service.stop()

    # Optional: Cancel other tasks if any were created directly in main
    # tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    # [task.cancel() for task in tasks]
    # logger.info(f"Cancelling {len(tasks)} outstanding tasks.")
    # await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Shutdown sequence initiated. Allowing run loop to finish cleanup...")
    # The run loop's finally block handles client closing now.

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