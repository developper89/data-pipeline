#!/usr/bin/env python3
# coap_connector/main.py
import logging
import asyncio
import signal
import sys
import os

from server import CoapGatewayServer
from command_consumer import CommandConsumer
from kafka_producer import KafkaMsgProducer
from shared.mq.kafka_helpers import create_kafka_producer
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

# Reduce kafka-python logging verbosity
logging.getLogger("kafka").setLevel(logging.WARNING)

# Set specific component log levels
logging.getLogger("server").setLevel(log_level)
logging.getLogger("command_consumer").setLevel(log_level)
logging.getLogger("shared.translation").setLevel(log_level)

logger = logging.getLogger(__name__)

class CoAPConnectorService:
    """Main CoAP connector service that manages both server and command consumer."""
    
    def __init__(self):
        self.coap_server = None
        self.command_consumer = None
        self.kafka_msg_producer = None
        self.running = False
        
    async def start(self):
        """Start the CoAP connector service."""
        logger.info("Starting CoAP Connector Service...")
        
        try:
            # Initialize Kafka producer
            logger.info("Creating Kafka producer...")
            self.kafka_msg_producer = KafkaMsgProducer(create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS))
            logger.info("Kafka producer created successfully")
            
            # Initialize command consumer
            self.command_consumer = CommandConsumer()
            await self.command_consumer.start()
            
            # Initialize CoAP server with required parameters
            logger.info(f"Creating CoAP server on {config.COAP_HOST}:{config.COAP_PORT}")
            self.coap_server = CoapGatewayServer(
                host=config.COAP_HOST,
                port=config.COAP_PORT,
                kafka_producer=self.kafka_msg_producer,
                command_consumer=self.command_consumer  # Pass the command consumer
            )
            
            self.running = True
            logger.info("CoAP Connector Service started successfully")
            
            # Start the CoAP server (sets up server, returns immediately)
            await self.coap_server.start()
            
            # Now start both the server wait task and command consumer concurrently
            await asyncio.gather(
                self.coap_server._run_task,  # Wait for server stop event
                self.command_consumer.consume_commands()  # Run command consumer
            )
            
        except Exception as e:
            logger.exception(f"Error in CoAP Connector Service: {e}")
            raise
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the CoAP connector service."""
        if not self.running:
            return
            
        logger.info("Stopping CoAP Connector Service...")
        self.running = False
        
        # Stop command consumer
        if self.command_consumer:
            try:
                await self.command_consumer.stop()
                logger.info("Command consumer stopped")
            except Exception as e:
                logger.error(f"Error stopping command consumer: {e}")
        
        # Stop CoAP server
        if self.coap_server:
            try:
                await self.coap_server.stop()
                logger.info("CoAP server stopped")
            except Exception as e:
                logger.error(f"Error stopping CoAP server: {e}")
        
        # Close Kafka producer
        if self.kafka_msg_producer:
            try:
                self.kafka_msg_producer.close(timeout=5)
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        
        logger.info("CoAP Connector Service stopped")

# Global service instance
service_instance = None

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}")
    if service_instance:
        # Set a flag to stop the service gracefully
        asyncio.create_task(service_instance.stop())

async def main():
    """Main entry point."""
    global service_instance
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start service
    service_instance = CoAPConnectorService()
    
    try:
        await service_instance.start()
        logger.info("CoAP Connector completed successfully")
    except KeyboardInterrupt:
        logger.info("CoAP Connector interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error in CoAP Connector: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())