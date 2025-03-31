# normalizer_service/service.py
import logging
import json
import asyncio # Needed if using async DB/Script clients
import signal
from typing import Optional, Dict, Any

from kafka import KafkaConsumer, KafkaProducer # Still need KafkaProducer for creation
from kafka.errors import KafkaError
from pydantic import ValidationError

# Import shared helpers, models, and local components
from shared.mq.kafka_helpers import create_kafka_consumer, create_kafka_producer # Keep helpers for creation
from shared.models.common import RawMessage, StandardizedOutput, ErrorMessage, generate_request_id
from shared.db.database import get_db_session # For async session management

# Import local components
from kafka_producer import NormalizerKafkaProducer # Import the new wrapper
from .db_client import DBClient, DeviceNotFoundError, MultipleDevicesFoundError
from .script_client import ScriptClient, ScriptNotFoundError
from .sandbox import get_sandbox, SandboxExecutionError, SandboxTimeoutError, SandboxResourceError
import config

logger = logging.getLogger(__name__)

class NormalizerService:
    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        # Use the dedicated wrapper for producing messages
        self.kafka_producer: Optional[NormalizerKafkaProducer] = None
        self.db_client = DBClient(session_factory=get_db_session) # Uses shared async session factory
        self.script_client = ScriptClient(
            storage_type=config.SCRIPT_STORAGE_TYPE,
            local_dir=config.LOCAL_SCRIPT_DIR
            # Add S3 args if configured
        )
        self.sandbox = get_sandbox()
        self._running = False
        self._stop_event = asyncio.Event() # Use asyncio event if using async components

    async def _process_message(self, raw_msg_record) -> bool:
        """
        Processes a single raw message from Kafka.
        Returns True if processing (or error publishing) was successful, False otherwise.
        """
        msg_value = raw_msg_record.value # Deserialized by KafkaConsumer
        msg_key = raw_msg_record.key.decode('utf-8') if raw_msg_record.key else None
        topic = raw_msg_record.topic
        partition = raw_msg_record.partition
        offset = raw_msg_record.offset

        logger.info(f"Processing msg from {topic=}, {partition=}, {offset=}, {msg_key=}")

        raw_message: Optional[RawMessage] = None
        job_dict: Optional[dict] = None # Keep original dict for error reporting
        request_id: Optional[str] = None
        error_published = False # Flag to track if an error was successfully published

        try:
            # 1. Deserialize and Validate Input RawMessage
            try:
                job_dict = msg_value # Already deserialized dict
                if 'request_id' not in job_dict:
                     job_dict['request_id'] = generate_request_id()
                raw_message = RawMessage(**job_dict)
                request_id = raw_message.request_id
                logger.debug(f"[{request_id}] Validated RawMessage for device: {raw_message.device_id}")
            except (ValidationError, TypeError) as e:
                logger.error(f"Invalid RawMessage format received: {e}. Value: {msg_value}")
                # Use the dedicated error publishing method
                await self._publish_processing_error("Invalid RawMessage format", error=str(e), original_message=msg_value, topic_details=(topic, partition, offset))
                error_published = True # Assume error publishing worked unless exception is raised
                return True # Indicate error was handled by publishing

            # 2. Fetch Device Configuration
            try:
                device_config_db = await self.db_client.get_device_config(raw_message.device_id)
                if not device_config_db:
                    raise DeviceNotFoundError(f"Config lookup returned None for {raw_message.device_id}")
                logger.debug(f"[{request_id}] Found config for {raw_message.device_id}: script={device_config_db.parser_script_ref}")
            except (DeviceNotFoundError, MultipleDevicesFoundError) as e:
                logger.error(f"[{request_id}] Configuration error for device '{raw_message.device_id}': {e}")
                await self._publish_processing_error(f"Configuration Error: {type(e).__name__}", error=str(e), original_message=job_dict, request_id=request_id, topic_details=(topic, partition, offset))
                error_published = True
                return True # Indicate error was handled
            except Exception as e: # Catch DB errors
                 logger.exception(f"[{request_id}] Database error fetching config for {raw_message.device_id}: {e}")
                 await self._publish_processing_error("Database Error", error=str(e), original_message=job_dict, request_id=request_id, topic_details=(topic, partition, offset))
                 error_published = True
                 return True # Assume non-retryable for now

            # 3. Fetch Parser Script Content
            try:
                script_ref = device_config_db.parser_script_ref
                script_module = await self.script_client.get_module(script_ref)
                logger.debug(f"[{request_id}] Fetched script content for ref: {script_ref}")
            except ScriptNotFoundError as e:
                 logger.error(f"[{request_id}] Script not found for device '{raw_message.device_id}': {e}")
                 await self._publish_processing_error("Script Not Found", error=str(e), original_message=job_dict, request_id=request_id, topic_details=(topic, partition, offset))
                 error_published = True
                 return True # Indicate error was handled
            except Exception as e: # Catch storage errors
                 logger.exception(f"[{request_id}] Storage error fetching script for {raw_message.device_id}: {e}")
                 await self._publish_processing_error("Script Storage Error", error=str(e), original_message=job_dict, request_id=request_id, topic_details=(topic, partition, offset))
                 error_published = True
                 return True # Assume non-retryable for now

            # 4. Execute script in Sandbox
            try:
                if hasattr(script_module, 'parse'):
                    print("Starting parser ...")
                    parser_output_dict = script_module.parse(raw_message.payload_b64)
                    logger.debug(f"[{request_id}] Sandbox execution completed.")
            except Exception as e: # Catch unexpected sandbox errors
                 logger.exception(f"[{request_id}] Unexpected sandbox error for {raw_message.device_id}: {e}")
                 await self._publish_processing_error("Unexpected Sandbox Error", error=str(e), original_message=job_dict, request_id=request_id, topic_details=(topic, partition, offset))
                 error_published = True
                 return True # Assume non-retryable

            # 5. Construct and Validate Standardized Output
            try:
                if 'device_id' not in parser_output_dict:
                    parser_output_dict['device_id'] = raw_message.device_id
                parser_output_dict['labels'] = device_config_db.labels
                parser_output_dict['request_id'] = request_id
                parser_output_dict['timestamp'] = raw_message.timestamp # Use timestamp from raw message

                standardized_data = StandardizedOutput(**parser_output_dict)
                logger.debug(f"[{request_id}] Validated standardized output.")
            except (ValidationError, TypeError) as e:
                logger.error(f"[{request_id}] Parser script output failed validation for device {raw_message.device_id}: {e}\nOutput: {parser_output_dict}")
                await self._publish_processing_error("Parser Output Validation Error", error=str(e), original_message=job_dict, request_id=request_id, details={"parser_output": parser_output_dict}, topic_details=(topic, partition, offset))
                error_published = True
                return True # Validation errors are not retryable
            except Exception as e: # Catch unexpected validation errors
                 logger.exception(f"[{request_id}] Unexpected validation error for {raw_message.device_id}: {e}")
                 await self._publish_processing_error("Unexpected Validation Error", error=str(e), original_message=job_dict, request_id=request_id, topic_details=(topic, partition, offset))
                 error_published = True
                 return True

            # 6. Publish Standardized Data to Kafka using the wrapper
            try:
                # Ensure producer wrapper is available
                if not self.kafka_producer:
                     logger.error(f"[{request_id}] Kafka producer wrapper not available. Cannot publish standardized data.")
                     return False # Indicate failure, cannot proceed

                self.kafka_producer.publish_standardized_data(standardized_data)
                # Success logging is now typically within the wrapper or can be added here if preferred
                logger.info(f"[{request_id}] Published standardized data for device {standardized_data.device_id}")
                return True # Success!

            except (KafkaError, Exception) as pub_err:
                 # Error is logged within the publish_standardized_data method now
                 # We just need to react to the failure
                 logger.error(f"[{request_id}] Failure occurred during standardized data publishing: {pub_err}. Message will be retried.")
                 return False # Indicate failure, message will be retried


        except Exception as e:
             # Catch-all for any unexpected error during the processing flow
             # before attempting to publish an error for it.
             request_id_for_log = request_id or 'Unknown'
             logger.exception(f"[{request_id_for_log}] UNHANDLED error during message processing: {e}. Raw value: {msg_value}")
             try:
                 # Attempt to publish this unhandled error
                 await self._publish_processing_error("Unhandled Processing Error", error=str(e), original_message=job_dict or msg_value, request_id=request_id, topic_details=(topic, partition, offset))
                 error_published = True
                 # Even though an error occurred, we *successfully published* an error message for it.
                 # So, we return True to commit the original message's offset and avoid reprocessing.
                 return True
             except Exception as pub_err:
                 # CRITICAL: Failed to process AND failed to publish the error.
                 logger.critical(f"[{request_id_for_log}] FAILED TO PUBLISH UNHANDLED ERROR TO KAFKA: {pub_err}. Original processing error: {e}")
                 # Returning False will cause Kafka to redeliver the message, potentially leading to a loop
                 # if the error is persistent. Consider alternative alerting/dead-letter queue strategy here.
                 return False # Indicate failure to process AND failure to publish error


    async def _publish_processing_error(self, error_type: str, error: str, original_message: Optional[dict | str] = None, request_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None, topic_details: Optional[tuple]=None):
        """
        Creates an ErrorMessage and publishes it via the Kafka producer wrapper.
        Raises an exception if the error publishing itself fails.
        """
        if not self.kafka_producer:
             # Log the original error here because we cannot publish it
             logger.error(f"[{request_id}] Cannot publish error '{error_type}' because Kafka producer wrapper not available. Original error: {error}")
             # Decide behaviour: Raise an exception? Or just log?
             # Raising makes the caller aware error reporting failed.
             raise RuntimeError(f"Kafka producer not available to publish error: {error_type}")

        request_id = request_id or generate_request_id() # Ensure we have an ID
        try:
            error_details = details or {}
            if topic_details:
                error_details["source_topic"] = topic_details[0]
                error_details["source_partition"] = topic_details[1]
                error_details["source_offset"] = topic_details[2]

            error_msg = ErrorMessage(
                request_id=request_id,
                service=config.SERVICE_NAME,
                error=f"{error_type}: {error}",
                original_message=original_message if isinstance(original_message, dict) else {"raw": str(original_message)}, # Ensure serializable
                details=error_details
            )

            # Call the wrapper method to publish
            # This might raise KafkaError or other exceptions if publishing fails
            self.kafka_producer.publish_error(error_msg)
            # Debug logging occurs within the wrapper's method now

        except Exception as pub_err:
            # Log critically that we failed to publish the error message
            logger.exception(f"CRITICAL: [{request_id}] Failed to publish error message via wrapper: {pub_err}. Original error type was: {error_type}")
            # Re-raise the exception so the caller knows error publishing failed
            raise pub_err


    async def run(self):
        """Main run loop: Connects to Kafka and processes messages."""
        self._running = True
        logger.info("Starting Normalizer Service...")

        while self._running: # Outer loop for Kafka client resilience
            raw_producer = None # Define here for visibility in finally block
            try:
                logger.info("Initializing Kafka clients...")
                self.consumer = create_kafka_consumer(
                    config.KAFKA_RAW_DATA_TOPIC,
                    config.KAFKA_CONSUMER_GROUP_ID,
                    config.KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset='earliest'
                )
                # Create the underlying producer instance
                raw_producer = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)

                # Check if clients were created successfully before wrapping
                if not self.consumer or not raw_producer:
                    raise RuntimeError("Failed to initialize Kafka clients. Check logs.")

                # Instantiate the producer wrapper
                self.kafka_producer = NormalizerKafkaProducer(raw_producer)

                logger.info(f"Consumer group '{config.KAFKA_CONSUMER_GROUP_ID}' waiting for messages on topic '{config.KAFKA_RAW_DATA_TOPIC}'...")

                while self._running: # Inner loop for message polling
                    msg_pack = self.consumer.poll(timeout_ms=config.KAFKA_CONSUMER_POLL_TIMEOUT_S * 1000)

                    if not msg_pack:
                        await asyncio.sleep(0.1)
                        continue

                    commit_needed = False
                    for tp, messages in msg_pack.items():
                        logger.debug(f"Received batch of {len(messages)} messages for {tp.topic} partition {tp.partition}")
                        for message in messages:
                            if not self._running: break # Check if stop was requested mid-batch

                            # Process each message
                            success = await self._process_message(message)

                            if success:
                                commit_needed = True # Mark that we need to commit *after* the batch
                            else:
                                # Processing failed AND error publishing likely failed (or wasn't attempted)
                                logger.error(f"Processing failed for msg at offset {message.offset} (partition {tp.partition}). Not committing offset. Message will be redelivered.")
                                commit_needed = False
                                break # Stop processing this partition's batch

                        if not self._running or not commit_needed:
                            break # Exit batch loop if stop requested or commit not needed

                    # Commit offsets ONLY if all messages in the polled batch were processed successfully
                    # AND if the service is still running.
                    if commit_needed and self._running:
                        try:
                             logger.debug("Committing Kafka offsets...")
                             self.consumer.commit() # Commit synchronously the offsets for all processed messages
                             logger.debug("Offsets committed.")
                        except KafkaError as commit_err:
                             logger.error(f"Failed to commit Kafka offsets: {commit_err}. Messages since last commit may be reprocessed.")
                             # Consider implications: stop service? Alert?

                    # Check stop event after processing a batch/poll
                    if self._stop_event.is_set():
                        self._running = False
                        logger.info("Stop event detected after processing batch.")
                        break # Exit inner polling loop

            except KafkaError as ke:
                 logger.error(f"KafkaError encountered in main loop: {ke}. Attempting to reconnect/reinitialize in 10 seconds...")
                 # Close existing clients safely before retrying
                 self._safe_close_clients()
                 await asyncio.sleep(10)
            except Exception as e:
                logger.exception(f"Fatal error in Normalizer run loop: {e}")
                self._running = False # Stop the service on unexpected errors
            finally:
                # Ensure clients are closed if loop exits for any reason
                # Check _running flag to avoid closing if we intend to retry connection
                if not self._running:
                    logger.info("Run loop ending, performing final client cleanup...")
                    self._safe_close_clients()
                # If raw_producer was created but wrapper wasn't, ensure it's closed
                elif raw_producer and not self.kafka_producer:
                    logger.warning("Closing raw producer instance as wrapper was not initialized.")
                    try:
                        raw_producer.close()
                    except Exception as e:
                         logger.error(f"Error closing raw producer during recovery: {e}")


        logger.info("Normalizer service run loop finished.")

    def _safe_close_clients(self):
        """Safely close Kafka consumer and producer wrapper, ignoring errors."""
        if self.consumer:
            try:
                logger.info("Closing Kafka consumer...")
                self.consumer.close()
            except Exception as e:
                logger.warning(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None

        if self.kafka_producer: # Check the wrapper instance
            try:
                logger.info("Closing Kafka producer wrapper...")
                self.kafka_producer.close(timeout=10) # Call wrapper's close
            except Exception as e:
                logger.warning(f"Error closing Kafka producer wrapper: {e}")
            finally:
                self.kafka_producer = None # Clear the wrapper instance
        else:
             logger.debug("Kafka producer wrapper was already closed or not initialized.")


    async def stop(self):
        """Signals the service to stop gracefully."""
        if self._stop_event.is_set():
            logger.info("Stop already requested.")
            return
        logger.info("Stop requested for Normalizer Service.")
        self._running = False
        self._stop_event.set()
        # Closing clients is handled in the finally block of the run loop upon exit.