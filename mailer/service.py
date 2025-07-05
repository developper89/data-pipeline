# mailer/service.py
import logging
import asyncio
import signal
import sys
from typing import Optional

from alert_consumer import AlertConsumer
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class MailerService:
    """
    Main mailer service that manages the alert consumer and handles graceful shutdown.
    """
    
    def __init__(self):
        """Initialize the mailer service."""
        self.alert_consumer: Optional[AlertConsumer] = None
        self.running = False
        self._shutdown_event = asyncio.Event()
        
    async def start(self):
        """Start the mailer service."""
        logger.info(f"Starting {config.SERVICE_NAME}")
        
        try:
            # Initialize alert consumer
            self.alert_consumer = AlertConsumer()
            
            # Start the alert consumer
            success = await self.alert_consumer.start()
            if not success:
                logger.error("Failed to start alert consumer")
                return False
            
            self.running = True
            logger.info(f"{config.SERVICE_NAME} started successfully")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
            return True
            
        except Exception as e:
            logger.exception(f"Error starting {config.SERVICE_NAME}: {e}")
            return False
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the mailer service."""
        if not self.running:
            return
            
        logger.info(f"Stopping {config.SERVICE_NAME}")
        self.running = False
        
        # Stop alert consumer
        if self.alert_consumer:
            try:
                await self.alert_consumer.stop()
                logger.info("Alert consumer stopped")
            except Exception as e:
                logger.error(f"Error stopping alert consumer: {e}")
        
        logger.info(f"{config.SERVICE_NAME} stopped")
    
    def shutdown(self):
        """Signal the service to shut down."""
        logger.info("Shutdown signal received")
        self._shutdown_event.set()
    
    def get_health_status(self) -> dict:
        """Get service health status."""
        status = {
            'service': config.SERVICE_NAME,
            'status': 'healthy' if self.running else 'unhealthy',
            'timestamp': asyncio.get_event_loop().time()
        }
        
        if self.alert_consumer:
            status['alert_consumer'] = {
                'running': self.alert_consumer.running,
                'statistics': self.alert_consumer.get_statistics()
            }
        
        return status

# Global service instance
service_instance: Optional[MailerService] = None

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}")
    if service_instance:
        service_instance.shutdown()

async def main():
    """Main entry point."""
    global service_instance
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start service
    service_instance = MailerService()
    
    try:
        success = await service_instance.start()
        if success:
            logger.info("Service completed successfully")
            sys.exit(0)
        else:
            logger.error("Service failed to start")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Unexpected error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 