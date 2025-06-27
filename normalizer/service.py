# normalizer_service/service.py
import logging
import json
import asyncio  # Needed if using async DB/Script clients
import os
import signal
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer  # Still need KafkaProducer for creation
from kafka.errors import KafkaError
from pydantic import ValidationError

# Import shared helpers, models, and local components
from shared.mq.kafka_helpers import (
    create_kafka_consumer,
    create_kafka_producer,
)  # Keep helpers for creation
from shared.models.common import (
    RawMessage,
    StandardizedOutput,
    ValidatedOutput,
    ErrorMessage,
    generate_request_id,
)
from preservarium_sdk.infrastructure.sql_repository.sql_parser_repository import (
    SQLParserRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import (
    SQLDatatypeRepository,
)

from preservarium_sdk.infrastructure.sql_repository.sql_sensor_repository import (
    SQLSensorRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_hardware_repository import (
    SQLHardwareRepository,
)


# Import local components
from kafka_producer import NormalizerKafkaProducer  # Import the new wrapper
from shared.utils.script_client import ScriptClient, ScriptNotFoundError
from normalizer.validator import Validator  # Import the validator
from shared.config_loader import get_translator_configs
from shared.translation.factory import TranslatorFactory

import config


logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class NormalizerService:
    def __init__(
        self, 
        parser_repository: SQLParserRepository,
        datatype_repository: Optional[SQLDatatypeRepository] = None,
        sensor_repository: Optional[SQLSensorRepository] = None,
        hardware_repository: Optional[SQLHardwareRepository] = None
    ):
        self.parser_repository = parser_repository
        self.datatype_repository = datatype_repository
        self.sensor_repository = sensor_repository
        self.hardware_repository = hardware_repository
        self.consumer: Optional[KafkaConsumer] = None
        # Use the dedicated wrapper for producing messages
        self.kafka_producer: Optional[NormalizerKafkaProducer] = None

        self.script_client = ScriptClient(
            storage_type=config.SCRIPT_STORAGE_TYPE, local_dir=config.LOCAL_SCRIPT_DIR
        )
        
        # Initialize validator if repositories are provided
        self.validator = None
        if datatype_repository:
            self.validator = Validator(datatype_repository)
            logger.info("Validator initialized with datatype repository")
        else:
            logger.warning("Validator not initialized - datatype repository not provided")
            
        self._running = False
        self._stop_event = (
            asyncio.Event()
        )  # Use asyncio event if using async components
        
        # Debug data collection
        self.debug_data: List[Dict[str, Any]] = []
        self.debug_file_path = "/app/normalizer/debug_messages.json"

    async def _process_message(self, raw_msg_record) -> bool:
        """
        Processes a single raw message from Kafka.
        Returns True if processing (or error publishing) was successful, False otherwise.
        """
        msg_value = raw_msg_record.value  # Deserialized by KafkaConsumer
        msg_key = raw_msg_record.key.decode("utf-8") if raw_msg_record.key else None
        topic = raw_msg_record.topic
        partition = raw_msg_record.partition
        offset = raw_msg_record.offset

        # logger.info(f"Processing msg from {topic=}, {partition=}, {offset=}, {msg_key=}")

        raw_message: Optional[RawMessage] = None
        job_dict: Optional[dict] = None  # Keep original dict for error reporting
        request_id: Optional[str] = None
        error_published = False  # Flag to track if an error was successfully published
        
        try:
            # 1. Deserialize and Validate Input RawMessage
            try:
                job_dict = msg_value  # Already deserialized dict
                if "request_id" not in job_dict:
                    job_dict["request_id"] = generate_request_id()
                raw_message = RawMessage(**job_dict)

                # Find the sensor using parameter field
                sensor = await self.sensor_repository.find_one_by(parameter=raw_message.device_id)
                if not sensor:
                    logger.warning(f"[{request_id}] No sensor found with parameter {raw_message.device_id}")
                    return True
                
                if not sensor.recording or not sensor.active:
                    logger.warning(f"[{request_id}] Sensor {raw_message.device_id} is not recording")
                    return True
                
                
                # Get the hardware configuration for the sensor
                hardware_config = await self._get_hardware_configuration(raw_message.device_id, request_id)
                
                
                
                # if raw_message.device_id == "2207001":
                #     logger.info(f"raw message device_id : {raw_message.device_id}, payload_hex: {raw_message.payload_hex}")
                request_id = raw_message.request_id
                # logger.info(f"device_id: {raw_message.device_id}, payload_hex: {raw_message.payload_hex}")
                # Collect debug data
                debug_entry = {
                    "device_id": raw_message.device_id,
                    "payload_hex": raw_message.payload_hex,
                    "request_id": request_id,
                    "timestamp": datetime.now().isoformat()
                }
                self.debug_data.append(debug_entry)
                
                # Save debug data every 100 entries to avoid memory buildup
                if len(self.debug_data) >= 20:
                    await self._save_debug_data()
                # logger.debug(
                #     f"[{request_id}] Validated RawMessage for device: {raw_message.device_id}"
                # )
            except (ValidationError, TypeError) as e:
                logger.error(
                    f"Invalid RawMessage format received: {e}. Value: {msg_value}"
                )
                # Use the dedicated error publishing method
                await self._publish_processing_error(
                    "Invalid RawMessage format",
                    error=str(e),
                    original_message=msg_value,
                    topic_details=(topic, partition, offset),
                )
                error_published = (
                    True  # Assume error publishing worked unless exception is raised
                )
                return True  # Indicate error was handled by publishing

            # 2. Fetch Device Configuration
            try:
                parser = await self.parser_repository.get_parser_for_sensor(
                    raw_message.device_id
                )
                if parser is None:
                    # logger.debug(f"[{request_id}] No config found for {raw_message.device_id}")
                    return True  # Indicate error was handled
                logger.info(
                    f"[{request_id}] Found config for {raw_message.device_id}: script={parser.file_path}"
                )

            except Exception as e:  # Catch DB errors
                logger.exception(
                    f"[{request_id}] Database error fetching config for {raw_message.device_id}: {e}"
                )
                await self._publish_processing_error(
                    "Database Error",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True  # Assume non-retryable for now

            # 3. Fetch Parser Script Content
            try:
                filename = os.path.basename(parser.file_path)
                script_ref = os.path.join(self.script_client.local_dir, filename)
                script_module = await self.script_client.get_module(script_ref)
                logger.debug(f"[{request_id}] Loaded script for ref: {script_ref}")
            except ScriptNotFoundError as e:
                logger.error(
                    f"[{request_id}] Script not found for device '{raw_message.device_id}': {e}"
                )
                await self._publish_processing_error(
                    "Script Not Found",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True  # Indicate error was handled
            except Exception as e:  # Catch storage errors
                logger.exception(
                    f"[{request_id}] Storage error fetching script for {raw_message.device_id}: {e}"
                )
                await self._publish_processing_error(
                    "Script Storage Error",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True  # Assume non-retryable for now

            # 4. Fetch Hardware Configuration
            hardware_config = await self._get_hardware_configuration(raw_message.device_id, request_id)

            # 5. Initialize Translator and Execute script in Sandbox
            try:
                if hasattr(script_module, "parse"):
                    logger.info(f"Starting parser module {script_module} ...")
                    
                    # Convert hex string payload back to bytes for parser
                    payload_bytes = bytes.fromhex(raw_message.payload_hex) if raw_message.payload_hex else b''
                    
                    # Initialize translator based on translator_used metadata
                    translator_instance = await self._initialize_translator(raw_message, request_id)
                    
                    if not translator_instance:
                        # If no translator, skip this message or use fallback
                        logger.warning(f"[{request_id}] No translator available for device {raw_message.device_id}")
                    
                    parser_output_list = script_module.parse(
                        payload=payload_bytes,  # Pass bytes to parser
                        translator=translator_instance,  # Pass the translator instance
                        metadata={
                            **raw_message.metadata,  # Pass the raw message metadata
                            'device_id': raw_message.device_id  # Include device_id in config
                        },
                        config=hardware_config
                    )
                    # logger.info(f"parser output: {parser_output_list}")
                    # logger.info(f"[{request_id}] Parser output for payload: {raw_message.payload}: {parser_output_list} ")
                    logger.debug(
                        f"[{request_id}] Script execution completed. Got {len(parser_output_list)} records."
                    )
                else:
                    logger.error(
                        f"[{request_id}] Script module does not have a 'parse' method."
                    )
                    await self._publish_processing_error(
                        "Script Missing Parse Method",
                        error="Script module does not have a 'parse' method",
                        original_message=job_dict,
                        request_id=request_id,
                        topic_details=(topic, partition, offset),
                    )
                    error_published = True
                    return True  # Assume non-retryable
            except Exception as e:  # Catch unexpected sandbox errors
                logger.exception(
                    f"[{request_id}] Unexpected script execution error for {raw_message.device_id}: {e}"
                )
                await self._publish_processing_error(
                    "Unexpected Script Error",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True  # Assume non-retryable

            # 6. Construct and Validate Standardized Output
            try:
                validated_data_list = []
                all_validation_errors = []
                
                # Get all datatypes for this device (via hardware association)
                
                device_datatypes = await self._get_device_datatypes(sensor, request_id)

                if device_datatypes:
                    logger.debug(f"[{request_id}] Found {len(device_datatypes)} datatypes for device {raw_message.device_id}")
                    
                    # Parse and validate each SensorReading object from the parser output
                    for sensor_reading in parser_output_list:
                        
                        # Ensure device_id is set if missing
                        if not hasattr(sensor_reading, 'device_id') or not sensor_reading.device_id:
                            sensor_reading["device_id"] = raw_message.device_id
                            
                        # Convert SensorReading to StandardizedOutput first
                        standardized_data = StandardizedOutput(
                            device_id=sensor_reading["device_id"],
                            values=sensor_reading["values"],
                            label=sensor_reading["label"],
                            index=sensor_reading["index"],
                            metadata=sensor_reading["metadata"],
                            request_id=request_id,
                            timestamp=getattr(sensor_reading, 'timestamp', raw_message.timestamp)
                        )
                            
                        # Apply validation using the index in the SensorReading
                        valid_outputs, validation_errors = await self._apply_validation(
                            raw_message.device_id,
                            device_datatypes,
                            standardized_data,
                            request_id
                        )
                        
                        # Add valid outputs to the list
                        validated_data_list.extend(valid_outputs)
                        
                        # Track validation errors
                        all_validation_errors.extend(validation_errors)
                
                # 5.2 Publish validation errors if any
                if all_validation_errors:
                    logger.warning(f"[{request_id}] Found {len(all_validation_errors)} validation errors")
                    for error in all_validation_errors:
                        # Add request_id if not already present
                        if not error.request_id:
                            error.request_id = request_id
                        # Publish error
                        self.kafka_producer.publish_error(error)
                
                if not validated_data_list:
                    # Handle case where all outputs were invalid
                    if all_validation_errors:
                        logger.error(f"[{request_id}] All outputs failed validation for device {raw_message.device_id}")
                        # We've already published individual errors, so we're done
                        return True
                    else:
                        # No valid outputs and no errors - this is strange
                        logger.error(f"[{request_id}] No valid outputs and no validation errors for device {raw_message.device_id}")
                        await self._publish_processing_error(
                            "Empty Validation Result",
                            error="No valid outputs produced and no validation errors reported",
                            original_message=job_dict,
                            request_id=request_id,
                            topic_details=(topic, partition, offset),
                        )
                        return True
                
                logger.debug(f"[{request_id}] Validated {len(validated_data_list)} validated outputs.")
            except (ValidationError, TypeError) as e:
                logger.error(
                    f"[{request_id}] Parser script output failed validation for device {raw_message.device_id}: {e}\nOutput: {parser_output_list}"
                )
                await self._publish_processing_error(
                    "Parser Output Validation Error",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    details={"parser_output": parser_output_list},
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True  # Validation errors are not retryable
            except Exception as e:  # Catch unexpected validation errors
                logger.exception(
                    f"[{request_id}] Unexpected validation error for {raw_message.device_id}: {e}"
                )
                await self._publish_processing_error(
                    "Unexpected Validation Error",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True

            # 7. Publish Standardized Data to Kafka using the wrapper
            try:
                # Ensure producer wrapper is available
                if not self.kafka_producer:
                    logger.error(
                        f"[{request_id}] Kafka producer wrapper not available. Cannot publish standardized data."
                    )
                    return False  # Indicate failure, cannot proceed

                for validated_data in validated_data_list:
                    self.kafka_producer.publish_validated_data(validated_data)
                    # Success logging is now typically within the wrapper or can be added here if preferred
                    logger.info(
                        f"[{request_id}] Published validated data for device {validated_data.device_id}"
                    )
                return True  # Success!

            except (KafkaError, Exception) as pub_err:
                # Error is logged within the publish_standardized_data method now
                # We just need to react to the failure
                logger.error(
                    f"[{request_id}] Failure occurred during standardized data publishing: {pub_err}. Message will be retried."
                )
                return False  # Indicate failure, message will be retried

        except Exception as e:
            # Catch-all for any unexpected error during the processing flow
            # before attempting to publish an error for it.
            request_id_for_log = request_id or "Unknown"
            logger.exception(
                f"[{request_id_for_log}] UNHANDLED error during message processing: {e}. Raw value: {msg_value}"
            )
            try:
                # Attempt to publish this unhandled error
                await self._publish_processing_error(
                    "Unhandled Processing Error",
                    error=str(e),
                    original_message=job_dict or msg_value,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                # Even though an error occurred, we *successfully published* an error message for it.
                # So, we return True to commit the original message's offset and avoid reprocessing.
                return True
            except Exception as pub_err:
                # CRITICAL: Failed to process AND failed to publish the error.
                logger.critical(
                    f"[{request_id_for_log}] FAILED TO PUBLISH UNHANDLED ERROR TO KAFKA: {pub_err}. Original processing error: {e}"
                )
                # Returning False will cause Kafka to redeliver the message, potentially leading to a loop
                # if the error is persistent. Consider alternative alerting/dead-letter queue strategy here.
                return False  # Indicate failure to process AND failure to publish error
                
    async def _get_device_datatypes(self, sensor: dict, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all datatypes for a device using the sensor repository and datatype repository.
        Returns a dictionary where keys are datatype_index values and values are datatype models.
        
        Args:
            device_id: The device ID (sensor parameter) to get datatypes for
            request_id: Optional request ID for logging
            
        Returns:
            Dictionary mapping datatype_index to datatype models
        """
        try:
                
            # Now get datatypes using the sensor's ID
            logger.info(f"[{request_id}] Found sensor with ID {sensor.id} for parameter {sensor.parameter}")
            datatypes = await self.datatype_repository.get_sensor_datatypes_ordered(sensor.id)
            
            # Create a map of datatype_index -> datatype
            datatype_map = {}
            for datatype in datatypes:
                if hasattr(datatype, 'datatype_index') and datatype.datatype_index:
                    datatype_map[datatype.datatype_index] = datatype
            
            return datatype_map
        except Exception as e:
            logger.error(f"[{request_id}] Error fetching datatypes for device {sensor.parameter}: {e}")
            return {}
            
    async def _apply_validation(
        self,
        device_id: str,
        datatype_map: Dict[str, Any],
        standardized_data: StandardizedOutput,
        request_id: Optional[str] = None
    ) -> Tuple[List[StandardizedOutput], List[ErrorMessage]]:
        """
        Apply validation to StandardizedOutput object using the correct datatype based on index.
        
        Args:
            device_id: The device ID
            datatype_map: Dictionary mapping datatype_index to datatype models
            standardized_data: StandardizedOutput object with device_id, values, index, etc.
            request_id: Optional request ID
            
        Returns:
            Tuple of valid standardized outputs and validation errors
        """
        valid_outputs = []
        all_errors = []
        
        # Get index from StandardizedOutput
        index = standardized_data.index
        
        try:
            # Find the matching datatype by index from the map
            if index and index in datatype_map:
                matching_datatype = datatype_map[index]
                logger.debug(f"[{request_id}] Found matching datatype {matching_datatype.id} with datatype_index {index}")
                
                # Validate against the found datatype - pass the datatype object directly
                validated_output, validation_errors = await self.validator.validate_and_normalize(
                    device_id=device_id,
                    standardized_data=standardized_data,
                    request_id=request_id,
                    datatype=matching_datatype
                )
                
                # Add validation errors to the list
                all_errors.extend(validation_errors)
                
                # Add valid output if any
                if validated_output:
                    valid_outputs.append(validated_output)
                
                return valid_outputs, all_errors
        except Exception as e:
            logger.error(f"[{request_id}] Error during validation lookup: {e}")
            error_msg = ErrorMessage(
                request_id=request_id,
                error=f"Validation lookup error: {str(e)}",
                original_message={"device_id": device_id, "standardized_data_index": standardized_data.index},
            )
            all_errors.append(error_msg)
                    
        return valid_outputs, all_errors

    async def _initialize_translator(self, raw_message: RawMessage, request_id: Optional[str] = None):
        """
        Initialize translator instance based on raw message metadata.
        
        Args:
            raw_message: The raw message containing metadata with translator_used
            request_id: Optional request ID for logging
            
        Returns:
            Translator instance or None if initialization fails
        """
        try:
            # Extract translator information from metadata
            translator_used = raw_message.metadata.get('translator_used')
            protocol = raw_message.protocol
            
            if not translator_used:
                logger.warning(f"[{request_id}] No translator_used found in metadata for device {raw_message.device_id}")
                return None
            
            logger.debug(f"[{request_id}] Initializing translator '{translator_used}' for protocol '{protocol}'")
            
            # Determine connector ID based on protocol
            connector_id = f"{protocol}-connector"
            
            # Get translator configurations for the appropriate connector
            translator_configs = get_translator_configs(connector_id)
            
            if not translator_configs:
                logger.warning(f"[{request_id}] No translator configurations found for connector '{connector_id}'")
                return None
            
            # Find the matching translator configuration
            matching_config = None
            for config_item in translator_configs:
                translator_type = config_item.get('type')
                config_details = config_item.get('config', {})
                manufacturer = config_details.get('manufacturer', '')
                
                # Construct expected translator_used name (e.g., "protobuf_efento")
                expected_name = f"{translator_type}_{manufacturer}" if manufacturer else translator_type
                
                if expected_name == translator_used:
                    matching_config = config_item
                    break
            
            if not matching_config:
                logger.warning(f"[{request_id}] No matching translator config found for '{translator_used}'")
                return None
            
            # Create translator instance using the factory
            translator_instance = TranslatorFactory.create_translator(matching_config)
            
            if translator_instance:
                logger.debug(f"[{request_id}] Successfully initialized translator '{translator_used}'")
                return translator_instance
            else:
                logger.error(f"[{request_id}] Failed to create translator instance for '{translator_used}'")
                return None
                
        except Exception as e:
            logger.exception(f"[{request_id}] Error initializing translator: {e}")
            return None

    async def _save_debug_data(self):
        """
        Save collected debug data to JSON file and clear the in-memory list.
        Overrides existing data for device_ids that already exist.
        """
        if not self.debug_data:
            return
            
        try:
            # Load existing data if file exists
            existing_data = []
            
            if os.path.exists(self.debug_file_path):
                try:
                    with open(self.debug_file_path, 'r') as f:
                        existing_data = json.load(f)
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"Could not read existing debug file: {e}. Starting fresh.")
                    existing_data = []
            
            # Create a dictionary mapping device_id to entry for easier lookup and replacement
            data_by_device_id = {}
            for entry in existing_data:
                device_id = entry.get('device_id')
                if device_id:
                    data_by_device_id[device_id] = entry
            
            # Process new entries - override existing or add new
            updated_count = 0
            new_count = 0
            
            for new_entry in self.debug_data:
                device_id = new_entry.get('device_id')
                if device_id:
                    if device_id in data_by_device_id:
                        # Override existing entry
                        data_by_device_id[device_id] = new_entry
                        updated_count += 1
                    else:
                        # Add new entry
                        data_by_device_id[device_id] = new_entry
                        new_count += 1
            
            # Convert back to list and save
            updated_data = list(data_by_device_id.values())
            
            with open(self.debug_file_path, 'w') as f:
                json.dump(updated_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Updated debug data: {updated_count} existing entries overridden, {new_count} new entries added to {self.debug_file_path}")
            
            # Clear the in-memory list
            self.debug_data.clear()
            
        except Exception as e:
            logger.error(f"Failed to save debug data: {e}")

    async def _publish_processing_error(
        self,
        error_type: str,
        error: str,
        original_message: Optional[dict | str] = None,
        request_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        topic_details: Optional[tuple] = None,
    ):
        """
        Creates an ErrorMessage and publishes it via the Kafka producer wrapper.
        Raises an exception if the error publishing itself fails.
        """
        if not self.kafka_producer:
            # Log the original error here because we cannot publish it
            logger.error(
                f"[{request_id}] Cannot publish error '{error_type}' because Kafka producer wrapper not available. Original error: {error}"
            )
            # Decide behaviour: Raise an exception? Or just log?
            # Raising makes the caller aware error reporting failed.
            raise RuntimeError(
                f"Kafka producer not available to publish error: {error_type}"
            )

        request_id = request_id or generate_request_id()  # Ensure we have an ID
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
                original_message=(
                    original_message
                    if isinstance(original_message, dict)
                    else {"raw": str(original_message)}
                ),  # Ensure serializable
                details=error_details,
            )

            # Call the wrapper method to publish
            # This might raise KafkaError or other exceptions if publishing fails
            self.kafka_producer.publish_error(error_msg)
            # Debug logging occurs within the wrapper's method now

        except Exception as pub_err:
            # Log critically that we failed to publish the error message
            logger.exception(
                f"CRITICAL: [{request_id}] Failed to publish error message via wrapper: {pub_err}. Original error type was: {error_type}"
            )
            # Re-raise the exception so the caller knows error publishing failed
            raise pub_err

    async def run(self):
        """Main run loop: Connects to Kafka and processes messages."""
        self._running = True
        logger.info("Starting Normalizer Service...")

        while self._running:  # Outer loop for Kafka client resilience
            raw_producer = None  # Define here for visibility in finally block
            try:
                logger.info("Initializing Kafka clients...")
                self.consumer = create_kafka_consumer(
                    config.KAFKA_RAW_DATA_TOPIC,
                    config.KAFKA_CONSUMER_GROUP_ID,
                    config.KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset="earliest",
                )
                # Create the underlying producer instance
                raw_producer = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)

                # Check if clients were created successfully before wrapping
                if not self.consumer or not raw_producer:
                    raise RuntimeError(
                        "Failed to initialize Kafka clients. Check logs."
                    )

                # Instantiate the producer wrapper
                self.kafka_producer = NormalizerKafkaProducer(raw_producer)

                logger.info(
                    f"Consumer group '{config.KAFKA_CONSUMER_GROUP_ID}' waiting for messages on topic '{config.KAFKA_RAW_DATA_TOPIC}'..."
                )

                while self._running:  # Inner loop for message polling
                    msg_pack = self.consumer.poll(
                        timeout_ms=config.KAFKA_CONSUMER_POLL_TIMEOUT_S * 1000
                    )

                    if not msg_pack:
                        await asyncio.sleep(0.1)
                        continue

                    commit_needed = False
                    for tp, messages in msg_pack.items():
                        logger.debug(
                            f"Received batch of {len(messages)} messages for {tp.topic} partition {tp.partition}"
                        )
                        for message in messages:
                            if not self._running:
                                break  # Check if stop was requested mid-batch

                            # Process each message
                            success = await self._process_message(message)

                            if success:
                                commit_needed = True  # Mark that we need to commit *after* the batch
                            else:
                                # Processing failed AND error publishing likely failed (or wasn't attempted)
                                logger.error(
                                    f"Processing failed for msg at offset {message.offset} (partition {tp.partition}). Not committing offset. Message will be redelivered."
                                )
                                commit_needed = False
                                break  # Stop processing this partition's batch

                        if not self._running or not commit_needed:
                            break  # Exit batch loop if stop requested or commit not needed

                    # Commit offsets ONLY if all messages in the polled batch were processed successfully
                    # AND if the service is still running.
                    if commit_needed and self._running:
                        try:
                            logger.debug("Committing Kafka offsets...")
                            self.consumer.commit()  # Commit synchronously the offsets for all processed messages
                            logger.debug("Offsets committed.")
                        except KafkaError as commit_err:
                            logger.error(
                                f"Failed to commit Kafka offsets: {commit_err}. Messages since last commit may be reprocessed."
                            )
                            # Consider implications: stop service? Alert?

                    # Check stop event after processing a batch/poll
                    if self._stop_event.is_set():
                        self._running = False
                        logger.info("Stop event detected after processing batch.")
                        break  # Exit inner polling loop

            except KafkaError as ke:
                logger.error(
                    f"KafkaError encountered in main loop: {ke}. Attempting to reconnect/reinitialize in 10 seconds..."
                )
                # Close existing clients safely before retrying
                self._safe_close_clients()
                await asyncio.sleep(10)
            except Exception as e:
                logger.exception(f"Fatal error in Normalizer run loop: {e}")
                self._running = False  # Stop the service on unexpected errors
            finally:
                # Ensure clients are closed if loop exits for any reason
                # Check _running flag to avoid closing if we intend to retry connection
                if not self._running:
                    logger.info("Run loop ending, performing final client cleanup...")
                    self._safe_close_clients()
                # If raw_producer was created but wrapper wasn't, ensure it's closed
                elif raw_producer and not self.kafka_producer:
                    logger.warning(
                        "Closing raw producer instance as wrapper was not initialized."
                    )
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

        if self.kafka_producer:  # Check the wrapper instance
            try:
                logger.info("Closing Kafka producer wrapper...")
                self.kafka_producer.close(timeout=10)  # Call wrapper's close
            except Exception as e:
                logger.warning(f"Error closing Kafka producer wrapper: {e}")
            finally:
                self.kafka_producer = None  # Clear the wrapper instance
        else:
            logger.debug(
                "Kafka producer wrapper was already closed or not initialized."
            )

    async def stop(self):
        """Signals the service to stop gracefully."""
        if self._stop_event.is_set():
            logger.info("Stop already requested.")
            return
        logger.info("Stop requested for Normalizer Service.")
        
        
        self._running = False
        self._stop_event.set()
        # Closing clients is handled in the finally block of the run loop upon exit.

    async def _get_hardware_configuration(self, device_id: str, request_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get hardware configuration for a device using the hardware repository.
        
        Args:
            device_id: The device ID (sensor parameter) to get hardware configuration for
            request_id: Optional request ID for logging
            
        Returns:
            Hardware configuration dictionary or None if not found
        """
        try:
                
            # Get the hardware directly by sensor parameter (device_id)
            hardware = await self.hardware_repository.get_hardware_by_sensor_parameter(device_id)
            if not hardware:
                logger.warning(f"[{request_id}] No hardware found for sensor parameter {device_id}")
                return {}
                
            logger.debug(f"[{request_id}] Found hardware configuration for device {device_id}: {hardware.name}")
            return hardware.configuration
            
        except Exception as e:
            logger.error(f"[{request_id}] Error fetching hardware configuration for device {device_id}: {e}")
            return {}
