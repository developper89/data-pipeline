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
from shared.mq.kafka_helpers import create_kafka_consumer

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
        self.consumer = None
        self.influx_writer = InfluxDBWriter()
        self.data_mapper = DataMapper()
        self._stop_event = asyncio.Event()
        
    async def initialize(self) -> bool:
        """
        Initialize the service components.
        Returns True if initialization successful, False otherwise.
        """
        logger.info("Initializing InfluxDB Ingestor Service")
        
        # Initialize Kafka consumer
        try:
            logger.info("Initializing Kafka consumer")
            self.consumer = create_kafka_consumer(
                config.KAFKA_VALIDATED_DATA_TOPIC,
                config.KAFKA_CONSUMER_GROUP_ID,
                config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest"
            )
            
            if not self.consumer:
                logger.error("Failed to create Kafka consumer")
                return False
                
            logger.info(f"Successfully subscribed to topic: {config.KAFKA_VALIDATED_DATA_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {str(e)}")
            return False
            
        # Initialize InfluxDB writer
        if not await self.influx_writer.connect():
            logger.error("Failed to connect to InfluxDB")
            return False
            
        # Ensure bucket exists
        if not await self.influx_writer.ensure_bucket_exists():
            logger.error("Failed to ensure InfluxDB bucket exists")
            return False
            
        logger.info("InfluxDB Ingestor Service initialized successfully")
        return True
        
    async def run(self):
        """
        Main service loop that consumes messages and processes them.
        """
        if not self.consumer:
            logger.error("Cannot run service: Kafka consumer not initialized")
            return
            
        self.running = True
        logger.info("Starting InfluxDB Ingestor Service processing loop")
        
        while self.running:  # Outer loop for Kafka client resilience
            try:
                logger.info("Waiting for messages from Kafka...")
                
                while self.running:  # Inner loop for message polling
                    # Poll for messages with timeout
                    msg_pack = self.consumer.poll(
                        timeout_ms=int(config.KAFKA_CONSUMER_POLL_TIMEOUT_S * 1000)
                    )
                    
                    if not msg_pack:
                        await asyncio.sleep(0.1)
                        continue
                        
                    commit_needed = False
                    for tp, messages in msg_pack.items():
                        logger.debug(f"Received batch of {len(messages)} messages for {tp.topic} partition {tp.partition}")
                        for message in messages:
                            if not self.running:
                                break  # Check if stop was requested mid-batch
                                
                            # Process the message
                            success = await self._process_message(message)
                            
                            if success:
                                commit_needed = True
                            else:
                                logger.error(f"Processing failed for message at offset {message.offset} (partition {tp.partition}). Not committing offset.")
                                commit_needed = False
                                break  # Stop processing this partition's batch
                                
                        if not self.running or not commit_needed:
                            break  # Exit batch loop if stop requested or commit not needed
                            
                    # Commit offsets if all messages were processed successfully
                    if commit_needed and self.running:
                        try:
                            logger.debug("Committing Kafka offsets...")
                            self.consumer.commit()  # Commit synchronously
                            logger.debug("Offsets committed.")
                        except KafkaError as commit_err:
                            logger.error(f"Failed to commit Kafka offsets: {commit_err}. Messages may be reprocessed.")
                            
                    # Check stop event after processing a batch
                    if self._stop_event.is_set():
                        self.running = False
                        logger.info("Stop event detected after processing batch.")
                        break  # Exit inner polling loop
                        
            except KafkaError as ke:
                logger.error(f"KafkaError encountered in main loop: {ke}. Attempting to reconnect in 10 seconds...")
                self._safe_close_consumer()
                await asyncio.sleep(10)
            except Exception as e:
                logger.exception(f"Unexpected error in processing loop: {str(e)}")
                self.running = False  # Stop the service on unexpected errors
            finally:
                if not self.running:
                    logger.info("Run loop ending, performing cleanup...")
                    self._safe_close_consumer()
                    
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
            
            # Check the persist field in metadata - skip if persist is False
            metadata = validated_output.metadata
            should_persist = metadata.get("persist", True)  # Default to True if not specified
            
            if not should_persist:
                logger.info(f"[{request_id}] Skipping data for device {validated_output.device_id} as persist=False, data: {validated_output.label}")
                return True  # Message processed successfully (by skipping)
                
            # Map data to InfluxDB points - pass the validated_output object directly
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
                return False  # Indicate failure - message will be retried
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return False  # Indicate failure - message will be retried
            
    def _safe_close_consumer(self):
        """Safely close the Kafka consumer, ignoring errors."""
        if self.consumer:
            try:
                logger.info("Closing Kafka consumer...")
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing Kafka consumer: {str(e)}")
            finally:
                self.consumer = None
                
    async def stop(self):
        """
        Stop the service and clean up resources.
        """
        if self._stop_event.is_set():
            logger.info("Stop already requested.")
            return
            
        logger.info("Stopping InfluxDB Ingestor Service")
        self.running = False
        self._stop_event.set()
        
        # Close InfluxDB connection
        logger.info("Closing InfluxDB connection")
        await self.influx_writer.close()
        
        # Consumer will be closed in the run loop's finally block
        logger.info("InfluxDB Ingestor Service stopped") 