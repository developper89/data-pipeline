import logging
import asyncio
from confluent_kafka import KafkaError, KafkaException
from shared.models.common import ValidatedOutput, generate_request_id
from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer
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
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        
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
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_VALIDATED_DATA_TOPIC,
            group_id=config.KAFKA_CONSUMER_GROUP_ID,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",  # Only cache latest messages
            seek_to_end=True,  # Seek to end of partitions for cache service (only latest data)
            on_error_callback=self._on_consumer_error
        )
        
        logger.info("Cache Service initialized successfully")
        return True
    
    async def run(self):
        """
        Main service loop that consumes messages and caches metadata using AsyncResilientKafkaConsumer.
        """
        if not self.resilient_consumer:
            logger.error("Cannot run service: AsyncResilientKafkaConsumer not initialized")
            return
            
        self.running = True
        logger.info("Starting Cache Service processing loop")
        
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_message,
                commit_offset=True,
                batch_processing=False  # Process messages individually
            )
        except Exception as e:
            logger.exception(f"Fatal error in cache service: {e}")
            self.running = False
        finally:
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
            data = message.parsed_value
            
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
            
            # Cache the validated output data
            try:
                await self.metadata_cache.cache_reading(validated_output)
                logger.debug(f"[{request_id}] Successfully cached reading for device {validated_output.device_id}")
                return True
            except Exception as e:
                logger.error(f"[{request_id}] Failed to cache reading for device {validated_output.device_id}: {str(e)}")
                return False  # Indicate failure - message will be retried
                
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
            
        logger.info("Stopping Cache Service")
        self.running = False
        self._stop_event.set()
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
        
        # Close Redis connection
        if self.redis_repository:
            await self.redis_repository.close()
        
        logger.info("Cache Service stopped") 