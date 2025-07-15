# shared/mq/kafka_helpers.py
import json
import logging
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import time
import socket
from datetime import datetime
import asyncio
from typing import Optional, Dict, Any, Callable, List, Union

logger = logging.getLogger(__name__)
logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
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
    """Get minimal consumer configuration for confluent-kafka compatibility."""
    config = {
        'bootstrap.servers': '',  # Will be set later
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Manual commit for better control
        'session.timeout.ms': 30000,   # 30 seconds
        'heartbeat.interval.ms': 10000,  # 10 seconds  
        'max.poll.interval.ms': 300000,  # 5 minutes
        'socket.keepalive.enable': True,
        'client.id': f'{group_id}_client',
        'security.protocol': 'PLAINTEXT',
        'statistics.interval.ms': 0,  # Disable statistics
    }
    
    # Apply any overrides
    config.update(overrides)
    return config

def get_optimized_producer_config(**overrides) -> Dict[str, Any]:
    """Get minimal producer configuration for confluent-kafka compatibility."""
    config = {
        'bootstrap.servers': '',  # Will be set later
        'acks': 'all',  # Wait for all replicas
        'retries': 10,  # Increased retries
        'request.timeout.ms': 120000,  # 2 minutes
        'delivery.timeout.ms': 300000,  # 5 minutes
        'socket.keepalive.enable': True,
        'security.protocol': 'PLAINTEXT',
        'client.id': 'producer_client',
        'statistics.interval.ms': 0,   # Disable statistics
    }
    
    # Apply any overrides
    config.update(overrides)
    return config

def create_kafka_producer(bootstrap_servers, base_delay=1, **config_overrides):
    """Creates a confluent-kafka Producer instance with enhanced error handling."""
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
            config['bootstrap.servers'] = validated_servers
            
            logger.debug(f"Creating Producer with config: {config}")
            producer = Producer(config)
            logger.info(f"Kafka Producer connected to {validated_servers}")
            return producer
            
        except KafkaException as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 6))  # Exponential backoff, max 64 seconds
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
    """Creates a confluent-kafka Consumer instance with enhanced error handling."""
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
            config = get_optimized_consumer_config(group_id, **config_overrides)
            config['bootstrap.servers'] = validated_servers
            config['auto.offset.reset'] = auto_offset_reset
            
            logger.debug(f"Creating Consumer with config: {config}")
            consumer = Consumer(config)
            
            # Subscribe to topic
            consumer.subscribe([topic])
            logger.info(f"Kafka Consumer connected to {validated_servers} for topic '{topic}', group '{group_id}'")
            return consumer
            
        except KafkaException as e:
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

def safe_kafka_poll(consumer: Consumer, timeout: float = 1.0) -> Optional[Any]:
    """
    Safely poll Kafka consumer with enhanced error handling.
    
    Args:
        consumer: Consumer instance
        timeout: Timeout in seconds
        
    Returns:
        Message or None, raises KafkaException on error
    """
    try:
        msg = consumer.poll(timeout=timeout)
        
        if msg is None:
            return None
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event, not an error
                logger.debug(f'Reached end of partition {msg.topic()}[{msg.partition()}] at offset {msg.offset()}')
                return None
            else:
                # Real error
                logger.error(f"Consumer error: {msg.error()}")
                raise KafkaException(msg.error())
        
        return msg
        
    except KafkaException as e:
        logger.error(f"Kafka poll error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Kafka poll: {e}")
        raise KafkaException(f"Unexpected poll error: {e}")

def safe_kafka_commit(consumer: Consumer, message=None, offsets=None, asynchronous=True) -> bool:
    """
    Safely commit Kafka consumer offsets with enhanced error handling.
    
    Args:
        consumer: Consumer instance
        message: Optional message to commit (commits message offset + 1)
        offsets: Optional list of TopicPartition to commit
        asynchronous: Whether to commit asynchronously
        
    Returns:
        True if commit successful, False otherwise
    """
    try:
        if message:
            # Commit specific message
            consumer.commit(message=message, asynchronous=asynchronous)
        elif offsets:
            # Commit specific offsets
            consumer.commit(offsets=offsets, asynchronous=asynchronous)
        else:
            # Commit all current offsets
            consumer.commit(asynchronous=asynchronous)
        return True
    except KafkaException as e:
        logger.error(f"Kafka commit error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during Kafka commit: {e}", exc_info=True)
        return False

def is_consumer_healthy(consumer: Consumer) -> bool:
    """
    Check if a Kafka consumer is healthy and able to communicate with brokers.
    
    Args:
        consumer: Consumer instance
        
    Returns:
        True if consumer is healthy, False otherwise
    """
    try:
        # Check cluster metadata to verify connectivity
        metadata = consumer.list_topics(timeout=10)
        if metadata is None:
            logger.warning("Consumer health check failed: unable to retrieve cluster metadata")
            return False
        
        # Check if we can see topics
        if len(metadata.topics) == 0:
            logger.warning("Consumer health check failed: no topics visible (possible connectivity issue)")
            return False
        
        # Check consumer assignment
        try:
            assignment = consumer.assignment()
            if assignment:
                logger.debug(f"Consumer assigned to {len(assignment)} partitions")
                
                # Check if we can get committed offsets for assigned partitions
                try:
                    committed = consumer.committed(assignment, timeout=5)
                    if committed is None:
                        logger.warning("Consumer health check failed: could not get committed offsets")
                        return False
                except Exception as commit_error:
                    logger.warning(f"Consumer health check failed: committed offset check failed: {commit_error}")
                    return False
            else:
                logger.debug("Consumer not yet assigned to any partitions")
        except Exception as assignment_error:
            logger.warning(f"Consumer health check failed during assignment check: {assignment_error}")
            return False
            
        logger.debug(f"Consumer health check passed: {len(metadata.topics)} topics visible, {len(consumer.assignment())} partitions assigned")
        return True
        
    except Exception as e:
        logger.warning(f"Consumer health check failed with exception: {e}")
        return False

def recreate_consumer_on_error(consumer: Consumer, topic: str, group_id: str, 
                              bootstrap_servers: str, **config_overrides) -> Optional[Consumer]:
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
        self.seek_to_end = seek_to_end
        self.config_overrides = config_overrides
        self.consumer = None
        self.retry_count = 0
        self.last_error = None
        self._stop_event = asyncio.Event()
        self._last_health_check = 0
        self._health_check_interval = 10  # Check health every 10 seconds
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
        """Setup consumer to seek to end of partitions."""
        if not self.consumer:
            return
            
        logger.info(f"Setting up seek to end for topic '{self.topic}'")
        
        # Wait for partition assignment
        max_wait_time = 30
        wait_start = time.time()
        
        while not self.consumer.assignment():
            if time.time() - wait_start > max_wait_time:
                logger.error(f"Timeout waiting for partition assignment for topic '{self.topic}'")
                return
                
            # Trigger partition assignment by polling briefly
            try:
                msg = self.consumer.poll(timeout=0.1)
                if msg and msg.error():
                    logger.warning(f"Error during partition assignment poll: {msg.error()}")
            except Exception as poll_error:
                logger.warning(f"Error during partition assignment poll: {poll_error}")
                return
                
            await asyncio.sleep(0.1)
        
        # Now seek to end
        try:
            assignment = self.consumer.assignment()
            logger.info(f"Partitions assigned for topic '{self.topic}': {assignment}")
            
            if assignment:
                # Get high water marks and seek to end
                for tp in assignment:
                    high_offset = self.consumer.get_watermark_offsets(tp)[1]
                    tp.offset = high_offset
                    self.consumer.seek(tp)
                logger.info(f"Successfully seeked to end of all partitions for topic '{self.topic}'")
                    
        except Exception as seek_error:
            logger.error(f"Error seeking to end for topic '{self.topic}': {seek_error}")
    
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
            message_handler: Async or sync function to process MessageWrapper objects
            commit_offset: Whether to commit offsets after successful processing
            batch_processing: If True, handler receives list of MessageWrapper objects instead of individual messages
            
        Note:
            The message handler receives MessageWrapper objects that contain:
            - 'message': The original confluent_kafka.Message object
            - 'parsed_value': The JSON-decoded message value
            - All other attributes are delegated to the original message
        """
        if not self.consumer:
            if not await self._create_consumer():
                logger.error(f"Failed to create consumer for topic '{self.topic}'")
                return
        
        self._running = True
        logger.info(f"AsyncResilientKafkaConsumer started for topic '{self.topic}'")
        
        consecutive_empty_polls = 0
        max_consecutive_empty_polls = 60  # 1 minute of empty polls
        message_batch = []
        
        while self._running and not self._stop_event.is_set():
            try:
                # Ensure consumer is available
                if not self.consumer:
                    logger.warning(f"Consumer is None for topic '{self.topic}', attempting to recreate...")
                    if not await self._create_consumer():
                        logger.error(f"Failed to recreate consumer for topic '{self.topic}', sleeping before retry...")
                        await asyncio.sleep(5)
                        continue
                
                # Periodic health check
                await self._check_consumer_health()
                
                # Poll for message
                msg = safe_kafka_poll(self.consumer, timeout=1.0)
                
                if msg:
                    consecutive_empty_polls = 0
                    
                    # Process message using confluent_kafka's native message object
                    try:
                        # Parse JSON value if present
                        msg_value = msg.value()
                        if msg_value is not None:
                            if isinstance(msg_value, bytes):
                                parsed_value = json.loads(msg_value.decode('utf-8'))
                            else:
                                parsed_value = json.loads(msg_value)
                        else:
                            parsed_value = None
                        
                        # Create a simple wrapper that contains both the message and parsed value
                        class MessageWrapper:
                            def __init__(self, message, parsed_value):
                                self.message = message
                                self.parsed_value = parsed_value
                                
                            # Delegate all other attributes to the original message
                            def __getattr__(self, name):
                                return getattr(self.message, name)
                        
                        wrapped_msg = MessageWrapper(msg, parsed_value)
                        
                        if batch_processing:
                            message_batch.append(wrapped_msg)
                            # Process batch when it reaches a certain size or timeout
                            if len(message_batch) >= 10:  # Process every 10 messages
                                try:
                                    if asyncio.iscoroutinefunction(message_handler):
                                        success = await message_handler(message_batch)
                                    else:
                                        success = message_handler(message_batch)
                                    
                                    if success and commit_offset:
                                        # Commit the last message in batch (use original message for commit)
                                        if not safe_kafka_commit(self.consumer, message=msg):
                                            logger.warning("Failed to commit batch offsets")
                                    
                                    message_batch.clear()
                                except Exception as e:
                                    logger.error(f"Error processing message batch: {e}")
                                    if self.on_error_callback:
                                        if asyncio.iscoroutinefunction(self.on_error_callback):
                                            await self.on_error_callback(e, message_batch)
                                        else:
                                            self.on_error_callback(e, message_batch)
                                    message_batch.clear()
                        else:
                            # Process individual message
                            try:
                                if asyncio.iscoroutinefunction(message_handler):
                                    success = await message_handler(wrapped_msg)
                                else:
                                    success = message_handler(wrapped_msg)
                                
                                if success and commit_offset:
                                    # Use original message for commit
                                    if not safe_kafka_commit(self.consumer, message=msg):
                                        logger.warning("Failed to commit message offset")
                                        await self._handle_consumer_error("Commit failure detected")
                                        continue
                                        
                            except Exception as e:
                                logger.error(f"Error processing message at offset {msg.offset()}: {e}")
                                if self.on_error_callback:
                                    if asyncio.iscoroutinefunction(self.on_error_callback):
                                        await self.on_error_callback(e, wrapped_msg)
                                    else:
                                        self.on_error_callback(e, wrapped_msg)
                                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to deserialize message at offset {msg.offset()}: {e}")
                        continue
                        
                else:
                    # No message received
                    consecutive_empty_polls += 1
                    
                    # Process any remaining batch messages on timeout
                    if batch_processing and message_batch and consecutive_empty_polls % 10 == 0:
                        try:
                            if asyncio.iscoroutinefunction(message_handler):
                                success = await message_handler(message_batch)
                            else:
                                success = message_handler(message_batch)
                            message_batch.clear()
                        except Exception as e:
                            logger.error(f"Error processing timeout batch: {e}")
                            message_batch.clear()
                    
                    if consecutive_empty_polls >= max_consecutive_empty_polls:
                        logger.warning(f"No messages received for {max_consecutive_empty_polls} consecutive polls, forcing health check")
                        if not is_consumer_healthy(self.consumer):
                            logger.warning("Consumer health check failed after consecutive empty polls, triggering reconnection")
                            await self._handle_consumer_error("Consumer unhealthy after consecutive empty polls")
                            continue
                        consecutive_empty_polls = 0
                    
                    await asyncio.sleep(0.1)
                                
            except KafkaException as e:
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
            self.retry_count = 0
        else:
            logger.error(f"Failed to recreate consumer on attempt {self.retry_count}")
    
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

def publish_message(producer: Producer, topic: str, value: dict, key: str = None):
    """Publishes a message to a Kafka topic with enhanced error handling."""
    try:
        # Convert Pydantic models to dict if needed
        if hasattr(value, '__dict__') and hasattr(value, 'model_dump'):
            value = model_to_dict(value)

        # Serialize value to JSON
        value_bytes = json.dumps(value, cls=DateTimeEncoder).encode('utf-8')
        key_bytes = key.encode('utf-8') if key else None
        
        # Produce message with callback
        def delivery_report(err, msg):
            if err is not None:
                logger.error(f'Message delivery failed for topic {topic}: {err}')
            else:
                logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        
        producer.produce(topic, value=value_bytes, key=key_bytes, callback=delivery_report)
        
        # Trigger delivery (non-blocking)
        producer.poll(0)
        
    except Exception as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        raise

def check_kafka_health(bootstrap_servers):
    """Check if Kafka brokers are healthy and reachable."""
    try:
        validated_servers = validate_bootstrap_servers(bootstrap_servers)
        if not validated_servers:
            return False, "No reachable Kafka brokers found"
        
        # Create admin client to test connectivity
        admin_config = {'bootstrap.servers': validated_servers}
        admin_client = AdminClient(admin_config)
        
        # Get cluster metadata
        metadata = admin_client.list_topics(timeout=10)
        
        return True, f"Successfully connected to Kafka brokers: {validated_servers}"
        
    except Exception as e:
        return False, f"Kafka health check failed: {e}"

def get_kafka_cluster_info(bootstrap_servers):
    """Get detailed information about the Kafka cluster."""
    try:
        validated_servers = validate_bootstrap_servers(bootstrap_servers)
        if not validated_servers:
            return None
        
        admin_config = {'bootstrap.servers': validated_servers}
        admin_client = AdminClient(admin_config)
        
        # Get cluster metadata
        metadata = admin_client.list_topics(timeout=10)
        
        cluster_info = {
            'bootstrap_servers': validated_servers,
            'topics': list(metadata.topics.keys()),
            'partitions': {},
            'brokers': []
        }
        
        # Get partition information for each topic
        for topic_name, topic_metadata in metadata.topics.items():
            cluster_info['partitions'][topic_name] = list(topic_metadata.partitions.keys())
        
        # Get broker information
        for broker_id, broker_metadata in metadata.brokers.items():
            cluster_info['brokers'].append({
                'id': broker_id,
                'host': broker_metadata.host,
                'port': broker_metadata.port
            })
        
        return cluster_info
        
    except Exception as e:
        logger.error(f"Failed to get cluster info: {e}")
        return None