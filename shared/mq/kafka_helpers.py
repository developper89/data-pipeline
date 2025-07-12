# shared/mq/kafka_helpers.py
import json
import logging
from kafka import KafkaProducer, KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
import time
import socket
from datetime import datetime
import asyncio
from typing import Optional, Dict, Any, Callable, List, Union

logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Handle bytes objects (often used in RawMessage.payload)
        elif isinstance(obj, bytes):
            return obj.hex()  # Convert bytes to hex string
        # Let the base class handle the rest
        return super().default(obj)

def model_to_dict(model):
    """Convert a Pydantic model to a dictionary with proper datetime handling."""
    if hasattr(model, 'model_dump'):  # Pydantic v2
        return model.model_dump()
    else:  # Pydantic v1
        return model.dict()

def validate_bootstrap_servers(bootstrap_servers):
    """Validate that bootstrap servers are reachable."""
    servers = bootstrap_servers.split(',')
    reachable_servers = []
    
    for server in servers:
        server = server.strip()
        try:
            if ':' in server:
                host, port = server.split(':')
                port = int(port)
            else:
                host = server
                port = 9092
            
            # Test DNS resolution
            try:
                socket.gethostbyname(host)
                logger.debug(f"DNS resolution successful for {host}")
            except socket.gaierror as e:
                logger.warning(f"DNS resolution failed for {host}: {e}")
                # Try common fallbacks for Docker environments
                if host == 'kafka':
                    fallback_hosts = ['localhost', '127.0.0.1']
                    for fallback in fallback_hosts:
                        try:
                            socket.gethostbyname(fallback)
                            logger.info(f"Using fallback {fallback}:{port} instead of {host}:{port}")
                            server = f"{fallback}:{port}"
                            host = fallback
                            break
                        except socket.gaierror:
                            continue
                    else:
                        logger.error(f"No fallback found for {host}")
                        continue
                else:
                    continue
            
            # Test port connectivity
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    reachable_servers.append(server)
                    logger.debug(f"Successfully connected to {host}:{port}")
                else:
                    logger.warning(f"Cannot connect to {host}:{port} (error code: {result})")
            except Exception as e:
                logger.warning(f"Connection test failed for {host}:{port}: {e}")
                
        except Exception as e:
            logger.error(f"Error validating server {server}: {e}")
    
    if not reachable_servers:
        logger.error(f"No reachable Kafka brokers found in: {bootstrap_servers}")
        return None
    
    validated_servers = ','.join(reachable_servers)
    if validated_servers != bootstrap_servers:
        logger.info(f"Updated bootstrap servers from {bootstrap_servers} to {validated_servers}")
    
    return validated_servers

def get_optimized_consumer_config(group_id: str, **overrides) -> Dict[str, Any]:
    """Get optimized consumer configuration to handle common connection issues."""
    config = {
        'enable_auto_commit': False,  # Manual commit for better control
        'request_timeout_ms': 120000,  # 2 minutes - increased from default
        'session_timeout_ms': 90000,   # 1.5 minutes - increased from default
        'heartbeat_interval_ms': 30000,  # 30 seconds - increased from default
        'metadata_max_age_ms': 60000,   # 1 minute - increased from default
        'reconnect_backoff_ms': 2000,   # 2 seconds - increased from default
        'reconnect_backoff_max_ms': 64000,  # 64 seconds max backoff
        'retry_backoff_ms': 1000,       # 1 second between retries
        'api_version_auto_timeout_ms': 30000,  # 30 seconds for API version detection
        'security_protocol': 'PLAINTEXT',
        'consumer_timeout_ms': 1000,    # Timeout for poll()
        # Fetch settings for better performance
        'fetch_min_bytes': 1,
        'fetch_max_wait_ms': 1000,      # Max wait time for fetch
        'max_partition_fetch_bytes': 1048576,  # 1MB per partition
        # Connection pool settings
        'connections_max_idle_ms': 300000,  # 5 minutes
        'max_poll_records': 500,
        'max_poll_interval_ms': 600000,     # 10 minutes max poll interval
        # Group coordination settings
        'group_id': group_id,
        'auto_offset_reset': 'earliest',
    }
    
    # Apply any overrides
    config.update(overrides)
    return config

def get_optimized_producer_config(**overrides) -> Dict[str, Any]:
    """Get optimized producer configuration to handle common connection issues."""
    config = {
        'retries': 10,  # Increased retries
        'acks': 'all',  # Wait for all replicas
        'request_timeout_ms': 120000,  # 2 minutes - increased from default
        'metadata_max_age_ms': 60000,  # 1 minute - increased from default
        'reconnect_backoff_ms': 2000,  # 2 seconds - increased from default
        'reconnect_backoff_max_ms': 64000,  # 64 seconds max backoff
        'retry_backoff_ms': 1000,      # 1 second between retries
        'max_block_ms': 30000,         # 30 seconds max block time
        'api_version_auto_timeout_ms': 30000,  # 30 seconds for API version detection
        'security_protocol': 'PLAINTEXT',
        # Buffer and batch settings for better performance
        'buffer_memory': 67108864,     # 64MB buffer - increased from default
        'batch_size': 32768,           # 32KB batch size - increased from default
        'linger_ms': 20,               # Wait 20ms for batching
        'compression_type': 'gzip',    # Compress messages
        # Connection settings
        'connections_max_idle_ms': 300000,  # 5 minutes
        'delivery_timeout_ms': 300000,      # 5 minutes total delivery timeout
    }
    
    # Apply any overrides
    config.update(overrides)
    return config

def create_kafka_producer(bootstrap_servers, base_delay=1, **config_overrides):
    """Creates a KafkaProducer instance with enhanced error handling and optimized configuration."""
    # Validate servers first
    validated_servers = validate_bootstrap_servers(bootstrap_servers)
    if not validated_servers:
        logger.warning(f"No reachable Kafka brokers found in: {bootstrap_servers}. Will keep retrying...")
        validated_servers = bootstrap_servers  # Use original servers and keep trying
    
    producer = None
    retry_count = 0
    
    while producer is None:
        try:
            # Get optimized configuration
            config = get_optimized_producer_config(**config_overrides)
            config['bootstrap_servers'] = validated_servers.split(',')
            config['value_serializer'] = lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8')
            
            producer = KafkaProducer(**config)
            logger.info(f"KafkaProducer connected to {validated_servers}")
            return producer
            
        except NoBrokersAvailable as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))  # Exponential backoff, max 64 seconds
            logger.error(f"No Kafka brokers available (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except KafkaError as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Kafka error creating producer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except Exception as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Unexpected error creating Kafka Producer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)


def create_kafka_consumer(topic, group_id, bootstrap_servers, auto_offset_reset='earliest', base_delay=1, **config_overrides):
    """Creates a KafkaConsumer instance with enhanced error handling and optimized configuration."""
    logger.info(f"Creating Kafka consumer for topic '{topic}', group '{group_id}', servers: {bootstrap_servers}")
    
    # Validate servers first
    validated_servers = validate_bootstrap_servers(bootstrap_servers)
    if not validated_servers:
        logger.warning(f"No reachable Kafka brokers found in: {bootstrap_servers}. Will keep retrying...")
        validated_servers = bootstrap_servers  # Use original servers and keep trying
    
    consumer = None
    retry_count = 0
    
    while consumer is None:
        try:
            # Get optimized configuration
            config = get_optimized_consumer_config(group_id, auto_offset_reset=auto_offset_reset, **config_overrides)
            config['bootstrap_servers'] = validated_servers.split(',')
            config['value_deserializer'] = lambda v: json.loads(v.decode('utf-8'))
            
            logger.debug(f"Creating KafkaConsumer with config: {config}")
            consumer = KafkaConsumer(topic, **config)
            logger.info(f"KafkaConsumer connected to {validated_servers} for topic '{topic}', group '{group_id}'")
            return consumer
            
        except NoBrokersAvailable as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"No Kafka brokers available for consumer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except KafkaError as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Kafka error creating consumer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
                
        except Exception as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))
            logger.error(f"Unexpected error creating Kafka Consumer (attempt {retry_count}): {e}")
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)


def safe_kafka_poll(consumer: KafkaConsumer, timeout_ms: int = 1000) -> Dict[Any, List[Any]]:
    """
    Safely poll Kafka consumer with enhanced error handling.
    
    Args:
        consumer: KafkaConsumer instance
        timeout_ms: Timeout in milliseconds
        
    Returns:
        Message batch dictionary, empty dict on error
        
    Raises:
        KafkaError: When connection/polling fails (to trigger reconnection)
    """
    try:
        return consumer.poll(timeout_ms=timeout_ms)
    except (KafkaError, KafkaTimeoutError) as e:
        logger.error(f"Kafka poll error: {e}")
        # Re-raise to trigger reconnection logic
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Kafka poll: {e}")
        # Re-raise as KafkaError to trigger reconnection
        raise KafkaError(f"Unexpected poll error: {e}")

def safe_kafka_commit(consumer: KafkaConsumer, offsets: Optional[Dict] = None) -> bool:
    """
    Safely commit Kafka consumer offsets with enhanced error handling.
    
    Args:
        consumer: KafkaConsumer instance
        offsets: Optional specific offsets to commit (can be {TopicPartition: offset} or {TopicPartition: OffsetAndMetadata})
        
    Returns:
        True if commit successful, False otherwise
    """
    try:
        if offsets:
            # Validate that offsets are in the correct format
            if not isinstance(offsets, dict):
                logger.warning(f"Invalid offsets format, expected dict: {type(offsets)}")
                return False
            
            # Check if all keys are TopicPartition objects
            for tp in offsets.keys():
                if not isinstance(tp, TopicPartition):
                    logger.warning(f"Invalid topic partition format in commit: {tp}")
                    return False
            
            logger.debug(f"Committing offsets: {offsets}")
            consumer.commit(offsets)
        else:
            consumer.commit()
        return True
    except KafkaError as e:
        logger.error(f"Kafka commit error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during Kafka commit: {e}", exc_info=True)
        return False

def is_consumer_healthy(consumer: KafkaConsumer) -> bool:
    """
    Check if a Kafka consumer is healthy and able to communicate with brokers.
    
    Args:
        consumer: KafkaConsumer instance
        
    Returns:
        True if consumer is healthy, False otherwise
    """
    try:
        # Primary check: Try to get cluster metadata - this will fail if connection is bad
        # Use a simple topic lookup to test connectivity
        topics = consumer.topics()
        if topics is None:
            logger.warning("Consumer health check failed: unable to retrieve topic metadata")
            return False
        
        # Secondary check: Verify we can see some topics (empty is suspicious)
        if isinstance(topics, set) and len(topics) == 0:
            logger.warning("Consumer health check failed: no topics visible (possible connectivity issue)")
            return False
        
        # Additional check: Verify group membership if consumer is assigned to partitions
        try:
            assignment = consumer.assignment()
            if assignment:
                # If we have partitions assigned, check if we can get high water marks
                # This will fail if consumer group membership is invalid
                for tp in list(assignment)[:1]:  # Just check one partition to avoid overhead
                    try:
                        consumer.highwater(tp)
                    except Exception as hw_error:
                        logger.warning(f"Consumer health check failed: highwater check failed for {tp}: {hw_error}")
                        return False
            
                    
        except Exception as assignment_error:
            logger.warning(f"Consumer health check failed during assignment check: {assignment_error}")
            return False
            
        # Additional check: Verify we can get committed offsets
        try:
            if consumer.assignment():
                consumer.committed(next(iter(consumer.assignment())))
        except Exception as commit_error:
            logger.warning(f"Consumer health check failed: committed offset check failed: {commit_error}")
            return False
            
        # If we can retrieve topics, the consumer is healthy
        # Note: bootstrap_connected() can be false even when consumer is working fine
        # after initial connection, so we don't check it anymore
        logger.debug(f"Consumer health check passed: {len(topics)} topics visible, {len(consumer.assignment())} partitions assigned")
        return True
        
    except Exception as e:
        logger.warning(f"Consumer health check failed with exception: {e}")
        return False

def recreate_consumer_on_error(consumer: KafkaConsumer, topic: str, group_id: str, 
                              bootstrap_servers: str, **config_overrides) -> Optional[KafkaConsumer]:
    """
    Recreate a consumer when it encounters errors.
    
    Args:
        consumer: Current consumer instance (will be closed)
        topic: Topic to subscribe to
        group_id: Consumer group ID
        bootstrap_servers: Kafka bootstrap servers
        **config_overrides: Additional configuration overrides
        
    Returns:
        New consumer instance or None if creation fails
    """
    # Close the existing consumer
    if consumer:
        try:
            consumer.close()
        except Exception as e:
            logger.warning(f"Error closing consumer during recreation: {e}")
    
    # Create new consumer
    try:
        return create_kafka_consumer(topic, group_id, bootstrap_servers, **config_overrides)
    except Exception as e:
        logger.error(f"Failed to recreate consumer: {e}")
        return None


class AsyncResilientKafkaConsumer:
    """An async-compatible resilient Kafka consumer wrapper that handles reconnections and errors gracefully."""
    
    def __init__(self, topic, group_id, bootstrap_servers, auto_offset_reset='earliest', 
                 max_retries=None, base_delay=1, on_error_callback=None, seek_to_end=False, **config_overrides):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.on_error_callback = on_error_callback
        self.seek_to_end = seek_to_end  # New parameter to seek to end after assignment
        self.config_overrides = config_overrides
        self.consumer = None
        self.retry_count = 0
        self.last_error = None
        self._stop_event = asyncio.Event()
        self._last_health_check = 0
        self._health_check_interval = 30  # Check health every 30 seconds
        self._running = False
        
    async def _create_consumer(self):
        """Create a new consumer instance."""
        try:
            logger.info(f"Creating new consumer for topic '{self.topic}' with group '{self.group_id}'")
            
            # Use asyncio.wait_for to add timeout to consumer creation
            self.consumer = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None,
                    create_kafka_consumer,
                    self.topic,
                    self.group_id,
                    self.bootstrap_servers,
                    self.auto_offset_reset,
                    self.base_delay,
                    **self.config_overrides
                ),
                timeout=30.0  # 30 second timeout for consumer creation
            )
            
            # If seek_to_end is enabled, wait for partition assignment and seek to end
            if self.seek_to_end and self.consumer:
                await self._setup_seek_to_end()
            
            self.retry_count = 0
            self.last_error = None
            logger.info(f"AsyncResilientKafkaConsumer successfully created for topic '{self.topic}'")
            return True
        except asyncio.TimeoutError:
            self.last_error = "Consumer creation timeout"
            logger.error(f"Timeout creating AsyncResilientKafkaConsumer for topic '{self.topic}' after 30 seconds")
            return False
        except Exception as e:
            self.last_error = e
            logger.error(f"Failed to create AsyncResilientKafkaConsumer for topic '{self.topic}': {e}")
            return False
    
    async def _setup_seek_to_end(self):
        """Setup consumer to seek to end of partitions (for cache services that only want latest data)."""
        if not self.consumer:
            return
            
        logger.info(f"Setting up seek to end for topic '{self.topic}'")
        
        # Wait for partition assignment with timeout
        max_wait_time = 30  # Maximum wait time in seconds
        wait_start = time.time()
        
        while not self.consumer.assignment():
            if time.time() - wait_start > max_wait_time:
                logger.error(f"Timeout waiting for partition assignment for topic '{self.topic}'")
                return
                
            # Trigger partition assignment by polling briefly
            try:
                self.consumer.poll(timeout_ms=100)
            except Exception as poll_error:
                logger.warning(f"Error during partition assignment poll: {poll_error}")
                return
                
            # Small async sleep to not block event loop
            await asyncio.sleep(0.1)
        
        # Now that partitions are assigned, seek to end
        try:
            assigned_partitions = self.consumer.assignment()
            logger.info(f"Partitions assigned for topic '{self.topic}': {assigned_partitions}")
            
            if assigned_partitions:
                self.consumer.seek_to_end()  # Seek to end of all assigned partitions
                logger.info(f"Successfully seeked to end of all partitions for topic '{self.topic}'")
            else:
                logger.warning(f"No partitions assigned for topic '{self.topic}'")
                
        except Exception as seek_error:
            logger.error(f"Error seeking to end for topic '{self.topic}': {seek_error}")
            # Don't fail consumer creation for seek errors, just log and continue
    
    async def _check_consumer_health(self):
        """Periodically check consumer health."""
        current_time = time.time()
        if current_time - self._last_health_check >= self._health_check_interval:
            self._last_health_check = current_time
            logger.debug(f"Performing health check for consumer on topic '{self.topic}'")
            if self.consumer and not is_consumer_healthy(self.consumer):
                logger.warning(f"Consumer health check failed for topic '{self.topic}', triggering reconnection")
                await self._handle_consumer_error("Health check failed")
            else:
                logger.debug(f"Consumer health check passed for topic '{self.topic}'")
    
    async def consume_messages(self, message_handler: Union[Callable, Callable[[Any], Any]], 
                              commit_offset: bool = True, batch_processing: bool = False):
        """
        Consume messages with automatic reconnection on failures.
        
        Args:
            message_handler: Async or sync function to process messages
            commit_offset: Whether to commit offsets after successful processing
            batch_processing: If True, handler receives entire message batch instead of individual messages
        """
        if not self.consumer:
            if not await self._create_consumer():
                return
        
        self._running = True
        logger.info(f"AsyncResilientKafkaConsumer started for topic '{self.topic}'")
        
        consecutive_empty_polls = 0
        max_consecutive_empty_polls = 300  # 5 minutes of empty polls before health check
        
        while self._running and not self._stop_event.is_set():
            try:
                # Ensure consumer is available before proceeding
                if not self.consumer:
                    logger.warning(f"Consumer is None for topic '{self.topic}', attempting to recreate...")
                    if not await self._create_consumer():
                        logger.error(f"Failed to recreate consumer for topic '{self.topic}', sleeping before retry...")
                        await asyncio.sleep(5)
                        continue
                
                # Periodic health check
                await self._check_consumer_health()
                
                # Use safe polling (now raises exceptions for reconnection)
                message_batch = safe_kafka_poll(self.consumer, timeout_ms=1000)
                
                if message_batch:
                    consecutive_empty_polls = 0  # Reset counter on successful poll
                    if batch_processing:
                        # Process entire batch at once
                        try:
                            if asyncio.iscoroutinefunction(message_handler):
                                success = await message_handler(message_batch)
                            else:
                                success = message_handler(message_batch)
                            
                            # Commit all offsets if successful
                            if success and commit_offset:
                                if not safe_kafka_commit(self.consumer):
                                    logger.warning(f"Failed to commit batch offsets")
                                    
                        except Exception as e:
                            logger.error(f"Error processing message batch: {e}")
                            if self.on_error_callback:
                                if asyncio.iscoroutinefunction(self.on_error_callback):
                                    await self.on_error_callback(e, message_batch)
                                else:
                                    self.on_error_callback(e, message_batch)
                    else:
                        # Process messages individually with batch-level commits
                        commit_needed = False
                        batch_failed = False
                        
                        for topic_partition, messages in message_batch.items():
                            if batch_failed:
                                break  # Stop processing if batch has failed
                                
                            logger.debug(f"Processing batch of {len(messages)} messages for {topic_partition.topic} partition {topic_partition.partition}")
                            
                            for message in messages:
                                if not self._running:
                                    batch_failed = True
                                    break  # Stop processing if service is stopping
                                    
                                try:
                                    # Process the message (support both sync and async handlers)
                                    if asyncio.iscoroutinefunction(message_handler):
                                        success = await message_handler(message)
                                    else:
                                        success = message_handler(message)
                                    
                                    if success:
                                        commit_needed = True  # Mark that we need to commit after the batch
                                    elif success is False:
                                        # Handler explicitly returned False, don't commit batch
                                        logger.warning(f"Message handler returned False for message at {message.offset}, not committing batch")
                                        commit_needed = False
                                        batch_failed = True
                                        break  # Stop processing this batch
                                    # If success is None, continue processing (neutral result)
                                        
                                except Exception as e:
                                    logger.error(f"Error processing message at offset {message.offset}: {e}")
                                    if self.on_error_callback:
                                        if asyncio.iscoroutinefunction(self.on_error_callback):
                                            await self.on_error_callback(e, message)
                                        else:
                                            self.on_error_callback(e, message)
                                    
                                    # Processing error - don't commit batch
                                    commit_needed = False
                                    batch_failed = True
                                    break  # Stop processing this batch
                            
                            if batch_failed:
                                break  # Exit outer loop if batch processing failed
                        
                        # Commit offsets ONLY if all messages in the batch were processed successfully
                        if commit_needed and commit_offset and self._running and not batch_failed:
                            try:
                                logger.debug("Committing Kafka offsets for processed batch...")
                                if not safe_kafka_commit(self.consumer):
                                    logger.warning("Failed to commit offsets for processed batch - triggering reconnection")
                                    # Commit failure indicates consumer is in bad state, trigger reconnection
                                    await self._handle_consumer_error("Commit failure detected")
                                    continue  # Skip to next iteration after error handling
                                else:
                                    logger.debug("Batch offsets committed successfully")
                            except Exception as commit_err:
                                logger.error(f"Error committing batch offsets: {commit_err}")
                                # Any commit exception should trigger reconnection
                                await self._handle_consumer_error(f"Commit exception: {commit_err}")
                                continue  # Skip to next iteration after error handling
                        elif batch_failed:
                            logger.warning("Batch processing failed, offsets not committed. Messages may be reprocessed.")
                else:
                    # No messages, increment counter and check for potential connection issues
                    consecutive_empty_polls += 1
                    
                    # If we've had too many consecutive empty polls, force a health check
                    if consecutive_empty_polls >= max_consecutive_empty_polls:
                        logger.warning(f"No messages received for {max_consecutive_empty_polls} consecutive polls, forcing health check")
                        if not is_consumer_healthy(self.consumer):
                            logger.warning("Consumer health check failed after consecutive empty polls, triggering reconnection")
                            await self._handle_consumer_error("Consumer unhealthy after consecutive empty polls")
                            continue
                        consecutive_empty_polls = 0  # Reset counter after health check
                    
                    # Sleep briefly
                    await asyncio.sleep(0.1)
                                
            except (KafkaError, KafkaTimeoutError) as e:
                logger.error(f"Kafka error in consumer loop for topic '{self.topic}': {e}")
                await self._handle_consumer_error(str(e))
                
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop for topic '{self.topic}': {e}")
                await self._handle_consumer_error(str(e))
                
        logger.info("AsyncResilientKafkaConsumer stopped")
    
    async def _handle_consumer_error(self, error):
        """Handle consumer errors with reconnection logic."""
        self.last_error = error
        self.retry_count += 1
        
        logger.error(f"Consumer error detected (attempt {self.retry_count}): {error}")
        
        if self.max_retries and self.retry_count > self.max_retries:
            logger.error(f"Max retries ({self.max_retries}) exceeded. Stopping consumer.")
            await self.stop()
            return
        
        # Close and recreate consumer
        if self.consumer:
            try:
                logger.info("Closing existing consumer connection...")
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer during recreation: {e}")
            self.consumer = None
        
        # Calculate delay before retry
        delay = self.base_delay * (2 ** min(self.retry_count - 1, 6))
        logger.info(f"Attempting consumer recreation in {delay} seconds...")
        await asyncio.sleep(delay)
        
        # Try to recreate consumer
        logger.info(f"Attempting to recreate consumer (attempt {self.retry_count})...")
        if await self._create_consumer():
            logger.info(f"Successfully recreated consumer after error (attempt {self.retry_count})")
            self.retry_count = 0  # Reset retry count on successful recreation
        else:
            logger.error(f"Failed to recreate consumer on attempt {self.retry_count}")
            # The retry will happen on the next iteration of the main loop
    
    async def stop(self):
        """Stop the consumer gracefully."""
        logger.info(f"Stopping AsyncResilientKafkaConsumer for topic '{self.topic}'")
        self._running = False
        self._stop_event.set()
        if self.consumer:
            try:
                logger.info(f"Closing consumer connection for topic '{self.topic}'")
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer for topic '{self.topic}': {e}")
            self.consumer = None
        logger.info(f"AsyncResilientKafkaConsumer stopped for topic '{self.topic}'")
    
    def is_healthy(self):
        """Check if the consumer is healthy."""
        return self.consumer is not None and self.last_error is None and is_consumer_healthy(self.consumer)


def publish_message(producer: KafkaProducer, topic: str, value: dict, key: str = None):
    """Publishes a message to a Kafka topic with enhanced error handling."""
    try:
        # Convert Pydantic models to dict if needed
        if hasattr(value, '__dict__') and hasattr(value, 'model_dump'):
            # This is likely a Pydantic model
            value = model_to_dict(value)

        key_bytes = key.encode('utf-8') if key else None
        future = producer.send(topic, value=value, key=key_bytes)
        
        # Wait for the message to be sent with timeout
        try:
            record_metadata = future.get(timeout=10)
            logger.debug(f"Published message to Kafka topic '{topic}', partition {record_metadata.partition}, offset {record_metadata.offset}")
        except Exception as e:
            logger.warning(f"Failed to get send confirmation for topic '{topic}': {e}")
            # Still flush to ensure message is sent
            producer.flush(timeout=5)
            logger.debug(f"Flushed message to Kafka topic '{topic}' with key '{key}'")

    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error publishing to Kafka topic '{topic}': {e}")
        raise

def check_kafka_health(bootstrap_servers):
    """Check if Kafka brokers are healthy and reachable."""
    try:
        validated_servers = validate_bootstrap_servers(bootstrap_servers)
        if not validated_servers:
            return False, "No reachable Kafka brokers found"
        
        # Try to create a temporary consumer to test connectivity
        config = get_optimized_consumer_config("health_check_group", consumer_timeout_ms=5000)
        config['bootstrap_servers'] = validated_servers.split(',')
        
        test_consumer = KafkaConsumer(**config)
        
        # Get metadata to verify connection
        topics = test_consumer.topics()
        test_consumer.close()
        
        return True, f"Successfully connected to Kafka brokers: {validated_servers}"
        
    except Exception as e:
        return False, f"Kafka health check failed: {e}"

def get_kafka_cluster_info(bootstrap_servers):
    """Get detailed information about the Kafka cluster."""
    try:
        validated_servers = validate_bootstrap_servers(bootstrap_servers)
        if not validated_servers:
            return None
        
        config = get_optimized_consumer_config("cluster_info_group", consumer_timeout_ms=5000)
        config['bootstrap_servers'] = validated_servers.split(',')
        
        test_consumer = KafkaConsumer(**config)
        
        cluster_info = {
            'bootstrap_servers': validated_servers,
            'topics': list(test_consumer.topics()),
            'partitions': {},
            'consumer_groups': []
        }
        
        # Get partition information for each topic
        for topic in cluster_info['topics']:
            try:
                partitions = test_consumer.partitions_for_topic(topic)
                cluster_info['partitions'][topic] = list(partitions) if partitions else []
            except Exception as e:
                logger.warning(f"Could not get partitions for topic {topic}: {e}")
                cluster_info['partitions'][topic] = []
        
        # Get consumer group information
        try:
            # Note: list_consumer_groups() is not available in this kafka-python version
            # Leaving empty for now as this is optional cluster info
            cluster_info['consumer_groups'] = []
        except Exception as e:
            logger.warning(f"Could not get consumer groups: {e}")
        
        test_consumer.close()
        return cluster_info
        
    except Exception as e:
        logger.error(f"Failed to get cluster info: {e}")
        return None