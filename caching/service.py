import logging
import asyncio
from kafka.errors import KafkaError
from shared.models.common import ValidatedOutput, generate_request_id
from shared.mq.kafka_helpers import create_kafka_consumer
import config

# Import the SDK components
from preservarium_sdk.core.config import RedisSettings
from preservarium_sdk.infrastructure.redis_repository.redis_base_repository import RedisBaseRepository

# Import local components
from caching.metadata_cache import MetadataCache

logger = logging.getLogger("cache_service.service")


class CacheService:
    """
    Main service class that consumes validated data from Kafka
    and caches complete reading data (values, label, index, metadata) in Redis.
    """
    
    def __init__(self):
        """Initialize the cache service."""
        self.running = False
        self.consumer = None
        
        # Initialize Redis cache components from the SDK
        redis_config = RedisSettings(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            metadata_ttl=config.REDIS_METADATA_TTL
        )
        
        # Create the Redis repository
        self.redis_repository = RedisBaseRepository(
            config=redis_config
        )
        
        # Create the device metadata cache service
        self.metadata_cache = MetadataCache(
            redis_repository=self.redis_repository
        )
        
        self._stop_event = asyncio.Event()
        
    async def initialize(self) -> bool:
        """
        Initialize the service components.
        Returns True if initialization is successful, False otherwise.
        """
        logger.info("Initializing Cache Service")
        
        # Initialize Redis connection
        redis_connected = await self.redis_repository.ensure_connected()
        if not redis_connected:
            logger.error("Failed to connect to Redis")
            return False
            
        # Initialize Kafka consumer
        try:
            logger.info("Initializing Kafka consumer")
            self.consumer = create_kafka_consumer(
                config.KAFKA_VALIDATED_DATA_TOPIC,
                config.KAFKA_CONSUMER_GROUP_ID,
                config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest"
            )
            
            if not self.consumer:
                logger.error("Failed to create Kafka consumer")
                return False
                
            logger.info(f"Successfully subscribed to topic: {config.KAFKA_VALIDATED_DATA_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {str(e)}")
            return False
            
        logger.info("Cache Service initialized successfully")
        return True
        
    async def run(self):
        """
        Main service loop that consumes messages and caches metadata.
        """
        if not self.consumer:
            logger.error("Cannot run service: Kafka consumer not initialized")
            return
            
        self.running = True
        logger.info("Starting Cache Service processing loop")
        
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
                        logger.info(f"Received batch of {len(messages)} messages for {tp.topic} partition {tp.partition}")

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
                    
        logger.info("Cache Service processing loop stopped")
        
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
                
            # Cache the complete reading data using cache_reading
            device_id = validated_output.device_id
            if not device_id:
                logger.warning("No device_id in validated output")
                return True  # Consider missing device_id as processed (won't retry)
            
            # Only cache if data exists and is not empty
            if not any([validated_output.values, validated_output.label, validated_output.index, validated_output.metadata]):
                logger.info(f"No meaningful data to cache for device {device_id}")
                return True  # Consider message without meaningful data as processed (won't retry)
            
            success = await self.metadata_cache.cache_reading(device_id, validated_output)
            
            if success:
                logger.info(f"Successfully cached complete reading for device {device_id}" +
                           (f" with request_id {validated_output.request_id}" if hasattr(validated_output, 'request_id') else ""))
            else:
                logger.error(f"Failed to cache complete reading for device {device_id}")
            
            # Always return True even if caching fails - we don't want to block the pipeline
            # for caching errors, just log them
            return True
                
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
            
        logger.info("Stopping Cache Service")
        self.running = False
        self._stop_event.set()
        
        # Close Redis connection
        await self.metadata_cache.close()
        
        # Consumer will be closed in the run loop's finally block
        logger.info("Cache Service stopped") 