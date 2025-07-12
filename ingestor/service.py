import logging
import json
import asyncio
from typing import Dict, Any, Optional, List
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import config
from influx_writer import InfluxDBWriter
from data_mapper import DataMapper
from shared.models.common import ValidatedOutput, generate_request_id
from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer

logger = logging.getLogger("ingestor.service")

class IngestorService:
    """
    Core ingestor service implementation that:
    1. Consumes messages from Kafka
    2. Maps them to InfluxDB data points
    3. Writes points to InfluxDB
    """
    
    def __init__(self):
        self.running = False
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        self.influx_writer = InfluxDBWriter()
        self.data_mapper = DataMapper()
        self._stop_event = asyncio.Event()
        
    async def initialize(self) -> bool:
        """
        Initialize the service components.
        Returns True if initialization successful, False otherwise.
        """
        logger.info("Initializing InfluxDB Ingestor Service")
        
        # Initialize InfluxDB writer
        if not await self.influx_writer.connect():
            logger.error("Failed to connect to InfluxDB")
            return False
            
        # Ensure bucket exists
        if not await self.influx_writer.ensure_bucket_exists():
            logger.error("Failed to ensure InfluxDB bucket exists")
            return False
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_VALIDATED_DATA_TOPIC,
            group_id=config.KAFKA_CONSUMER_GROUP_ID,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            on_error_callback=self._on_consumer_error
        )
        
        logger.info("InfluxDB Ingestor Service initialized successfully")
        return True
        
    async def run(self):
        """
        Main service loop that consumes messages and processes them using AsyncResilientKafkaConsumer.
        """
        if not self.resilient_consumer:
            logger.error("Cannot run service: AsyncResilientKafkaConsumer not initialized")
            return
            
        self.running = True
        logger.info("Starting InfluxDB Ingestor Service processing loop")
        
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_message,
                commit_offset=True,
                batch_processing=False  # Process messages individually
            )
        except Exception as e:
            logger.exception(f"Fatal error in ingestor service: {e}")
            self.running = False
        finally:
            # Clean up
            logger.info("InfluxDB Ingestor Service processing loop stopped")
            
    async def _process_message(self, message) -> bool:
        """
        Process a single Kafka message.
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Message value is already deserialized by KafkaConsumer
            data = message.value
            
            request_id = data.get("request_id", generate_request_id())
            
            logger.debug(f"[{request_id}] Processing message for device: {data.get('device_id', 'unknown')}")
            
            # Validate message is a ValidatedOutput
            try:
                # Create a ValidatedOutput instance to validate the structure
                validated_output = ValidatedOutput(**data)
                logger.debug(f"[{request_id}] Parsed ValidatedOutput for device: {validated_output.device_id}")
            except Exception as e:
                logger.error(f"[{request_id}] Invalid ValidatedOutput format: {str(e)}")
                return True  # Consider invalid message as processed (won't retry)
            
            # Map data to InfluxDB points - pass the validated_output object directly
            # Note: The data_mapper will handle per-value persistence filtering
            points = self.data_mapper.map_data(validated_output)
            if not points:
                logger.warning(f"[{request_id}] No valid points mapped from message, skipping")
                return True  # Consider message as processed even if no points were generated
                
            # Write points to InfluxDB
            success = await self.influx_writer.write_points(points)
            if success:
                logger.debug(f"[{request_id}] Successfully wrote {len(points)} points to InfluxDB for device {validated_output.device_id}")
                return True
            else:
                logger.error(f"[{request_id}] Failed to write points to InfluxDB for device {validated_output.device_id}")
                return True  # Indicate failure - message will be retried
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            return False  # Indicate failure - message will be retried
    
    async def _on_consumer_error(self, error, context):
        """Handle consumer-level errors."""
        logger.error(f"Consumer error: {error}")
        # Could implement additional error handling logic here if needed
                
    async def stop(self):
        """
        Stop the service and clean up resources.
        """
        if not self.running:
            logger.info("Stop already requested.")
            return
            
        logger.info("Stopping InfluxDB Ingestor Service")
        self.running = False
        self._stop_event.set()
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
        
        # Close InfluxDB connection
        logger.info("Closing InfluxDB connection")
        await self.influx_writer.close()
        
        logger.info("InfluxDB Ingestor Service stopped") 