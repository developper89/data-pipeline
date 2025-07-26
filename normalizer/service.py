# normalizer_service/service.py
import logging
import json
import asyncio  # Needed if using async DB/Script clients
import os
import signal
import inspect  # For parser signature detection
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

# Updated to use confluent-kafka through shared helpers
from confluent_kafka import KafkaError, KafkaException
from pydantic import ValidationError

# Import shared helpers, models, and local components
from shared.mq.kafka_helpers import (
    create_kafka_producer,
    AsyncResilientKafkaConsumer,
)  # Use AsyncResilientKafkaConsumer instead of create_kafka_consumer
from shared.models.common import (
    RawMessage,
    StandardizedOutput,
    ValidatedOutput,
    ErrorMessage,
    CommandMessage,
    generate_request_id,
)
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import (
    SQLDatatypeRepository,
)

from preservarium_sdk.infrastructure.sql_repository.sql_sensor_repository import (
    SQLSensorRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_broker_repository import (
    SQLBrokerRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_hardware_repository import (
    SQLHardwareRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_alarm_repository import (
    SQLAlarmRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_alert_repository import (
    SQLAlertRepository,
)
from preservarium_sdk.infrastructure.sql_repository.sql_user_repository import (
    SQLUserRepository,
)

# Import local components
from kafka_producer import NormalizerKafkaProducer  # Import the new wrapper
from shared.utils.script_client import ScriptClient, ScriptNotFoundError
from normalizer.validator import Validator  # Import the validator
from normalizer.alarm_handler import AlarmHandler  # Import the alarm handler
from normalizer.cache_context import CacheContext  # Import the new cache context
from shared.config_loader import get_translator_configs
from shared.translation.factory import TranslatorFactory

import config


logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class NormalizerService:
    def __init__(
        self, 
        datatype_repository: Optional[SQLDatatypeRepository] = None,
        sensor_repository: Optional[SQLSensorRepository] = None,
        hardware_repository: Optional[SQLHardwareRepository] = None,
        alarm_repository: Optional[SQLAlarmRepository] = None,
        alert_repository: Optional[SQLAlertRepository] = None,
        user_repository: Optional[SQLUserRepository] = None,
        broker_repository: Optional[SQLBrokerRepository] = None,
        sensor_cache_service=None  # New parameter for cache context
    ):
        self.datatype_repository = datatype_repository
        self.sensor_repository = sensor_repository
        self.hardware_repository = hardware_repository
        self.alarm_repository = alarm_repository
        self.alert_repository = alert_repository
        self.user_repository = user_repository
        self.broker_repository = broker_repository
        # Use AsyncResilientKafkaConsumer instead of manual consumer management
        self.resilient_consumer = None
        self.kafka_producer: Optional[NormalizerKafkaProducer] = None

        self.script_client = ScriptClient(
            storage_type=config.SCRIPT_STORAGE_TYPE, local_dir=config.LOCAL_SCRIPT_DIR
        )
        
        # Initialize cache context if sensor cache service is provided
        self.cache_context = None
        if sensor_cache_service:
            self.cache_context = CacheContext(sensor_cache_service)
            logger.info("Cache context initialized for parser scripts")
        else:
            logger.info("Cache context not initialized - sensor cache service not provided")
        
        # Initialize validator if repositories are provided
        self.validator = None
        if datatype_repository:
            self.validator = Validator(datatype_repository)
            logger.info("Validator initialized with datatype repository")
        else:
            logger.warning("Validator not initialized - datatype repository not provided")
            
        # Alarm handler will be initialized in run() method with admin user
        self.alarm_handler = None
            
        self._running = False
        self._stop_event = asyncio.Event()
        
        # Debug data collection
        self.debug_data: List[Dict[str, Any]] = []
        self.debug_file_path = "/app/normalizer/debug_messages.json"
        
        # Translator instance cache to avoid recreating translators for each message
        self.translator_cache: Dict[str, Any] = {}
        
    async def _initialize_alarm_handler(self):
        """Initialize the alarm handler with all required dependencies including admin user."""
        if self.alarm_repository and self.alert_repository and self.datatype_repository:
            try:
                # Get admin user if user repository is available
                admin_user = None
                if self.user_repository:
                    admin_user = await self.user_repository.get_first_admin_user()
                    if admin_user:
                        logger.info(f"Admin user found for alarm handler: {admin_user.username}")
                    else:
                        logger.warning("No admin user found in the system")
                
                # Create alarm handler with all dependencies
                self.alarm_handler = AlarmHandler(
                    alarm_repository=self.alarm_repository,
                    alert_repository=self.alert_repository,
                    datatype_repository=self.datatype_repository,
                    admin_user=admin_user,
                    kafka_producer=self.kafka_producer
                )
                
                logger.info("Alarm handler initialized with all dependencies")
                
            except Exception as e:
                logger.error(f"Failed to initialize alarm handler: {e}")
                # Continue without alarm handler
                self.alarm_handler = None
        else:
            logger.warning("Alarm handler not initialized - required repositories not provided")

    async def _process_message(self, raw_msg_record) -> bool:
        """
        Processes a single raw message from Kafka.
        Returns True if processing (or error publishing) was successful, False otherwise.
        """
        # Use the parsed_value from MessageWrapper (already deserialized JSON)
        msg_value = raw_msg_record.parsed_value
        
        # Handle key properly - it might be bytes or string
        msg_key = None
        if raw_msg_record.key():
            key_data = raw_msg_record.key()
            if isinstance(key_data, bytes):
                msg_key = key_data.decode("utf-8")
            else:
                msg_key = key_data
        
        # Get topic, partition, offset from confluent_kafka methods
        topic = raw_msg_record.topic()
        partition = raw_msg_record.partition()
        offset = raw_msg_record.offset()

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

                # Initialize hardware_config as default empty dict
                hardware_config = {}
                device = None

                # Find the sensor using parameter field
                if raw_message.device_type == "broker":
                    device = await self.broker_repository.find_one_by(parameter=raw_message.device_id)
                    if not device:
                        logger.debug(f"[{request_id}] No broker found with parameter {raw_message.device_id}")
                        return True
                    hardware_config = {}
                
                else:
                    device = await self.sensor_repository.find_one_by(parameter=raw_message.device_id)
                    if not device:
                        logger.debug(f"[{request_id}] No sensor found with parameter {raw_message.device_id}")
                        return True
                    if not device.recording or not device.active:
                        logger.debug(f"[{request_id}] Sensor {raw_message.device_id} is not recording")
                        return True

                    hardware_config = await self._get_hardware_configuration(raw_message.device_id, request_id)
                    # For other device types (light_controller, etc.), use empty config
                    # logger.debug(f"[{request_id}] Device type '{raw_message.device_type}' using default empty config")

                
                
                
                
                # if raw_message.device_id == "2207001":
                #     logger.info(f"raw message device_id : {raw_message.device_id}, payload_hex: {raw_message.payload_hex}")
                request_id = raw_message.request_id
                # logger.info(f"device_id: {raw_message.device_id}, payload_hex: {raw_message.payload_hex}")
                # Collect debug data
                debug_entry = {
                    "device_id": raw_message.device_id,
                    "payload": raw_message.payload,
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

            # 2. Get Script Module from Translator
            try:
                # Initialize translator based on translator_used metadata
                translator_instance = await self._initialize_translator(raw_message, request_id)
                
                if not translator_instance:
                    logger.warning(f"[{request_id}] No translator available for device {raw_message.device_id}")
                    return True  # Indicate error was handled
                
                # Load script module from translator if available
                script_module = None
                if hasattr(translator_instance, 'script_module') and translator_instance.script_module:
                    script_module = translator_instance.script_module
                elif hasattr(translator_instance, 'parser_script_path') and translator_instance.parser_script_path:
                    # Try to load the script module if it's not already loaded
                    try:
                        await translator_instance.load_script_module_async()
                        script_module = translator_instance.script_module
                    except Exception as e:
                        logger.error(f"[{request_id}] Failed to load script module for translator: {e}")
                
                if not script_module:
                    logger.debug(f"[{request_id}] No script module available for device {raw_message.device_id}")
                    return True  # Indicate error was handled
                    
                logger.debug(f"[{request_id}] Successfully loaded script module for device {raw_message.device_id}")

            except Exception as e:  # Catch configuration errors
                logger.exception(f"[{request_id}] Error getting script module for {raw_message.device_id}: {e}")
                await self._publish_processing_error(
                    "Script Module Error",
                    error=str(e),
                    original_message=job_dict,
                    request_id=request_id,
                    topic_details=(topic, partition, offset),
                )
                error_published = True
                return True  # Assume non-retryable for now

            # 3. Fetch Hardware Configuration
            

            # 4. Execute Parser Script with Translator
            try:
                # Get the action from RawMessage attribute, default to "parse" if not specified
                action = raw_message.action or "parse"
                
                if hasattr(script_module, action):
                    
                    # Convert hex string payload back to bytes for parser
                    # payload_bytes = bytes.fromhex(raw_message.payload_hex) if raw_message.payload_hex else b''
                    payload_bytes = raw_message.payload
                    
                    # Get the method to call dynamically
                    action_method = getattr(script_module, action)
                    
                    # Check if parser supports cache context (backwards compatibility)
                    parser_signature = inspect.signature(action_method)
                    supports_cache_context = 'cache_context' in parser_signature.parameters
                    
                    # Prepare metadata for parser
                    parser_metadata = {
                        **raw_message.metadata,  # Pass the raw message metadata
                        'device_id': raw_message.device_id  # Include device_id in metadata
                    }
                    
                    # Check if parser is async (for cache-aware parsers)
                    is_async_parser = asyncio.iscoroutinefunction(action_method)
                    # Call parser with or without cache context
                    if supports_cache_context and self.cache_context:
                        logger.info(f"[{request_id}] Calling parser with cache context for device {raw_message.device_id}")
                        if is_async_parser:
                            action_result = await action_method(
                                payload=payload_bytes,
                                metadata=parser_metadata,
                                config=hardware_config,
                                message_parser=getattr(translator_instance, 'message_parser', None),
                                cache_context=self.cache_context  # NEW: Pass cache context
                            )
                        else:
                            action_result = action_method(
                                payload=payload_bytes,
                                metadata=parser_metadata,
                                config=hardware_config,
                                message_parser=getattr(translator_instance, 'message_parser', None),
                                cache_context=self.cache_context  # NEW: Pass cache context
                            )
                    else:
                        # Backwards compatibility for parsers that don't support cache context
                        if not supports_cache_context:
                            logger.debug(f"[{request_id}] Parser does not support cache context")
                        elif not self.cache_context:
                            logger.debug(f"[{request_id}] Cache context not available")
                        
                        if is_async_parser:
                            action_result = await action_method(
                                payload=payload_bytes,
                                metadata=parser_metadata,
                                config=hardware_config,
                                message_parser=getattr(translator_instance, 'message_parser', None)
                            )
                        else:
                            action_result = action_method(
                                payload=payload_bytes,
                                metadata=parser_metadata,
                                config=hardware_config,
                                message_parser=getattr(translator_instance, 'message_parser', None)
                            )
                    
                    # Handle different response formats
                    if isinstance(action_result, tuple) and len(action_result) == 2:
                        # New format: (response_type, data)
                        response_type, data = action_result
                        logger.debug(f"[{request_id}] Script execution completed using action '{action}' with response_type '{response_type}'")
                        
                        if response_type == "data":
                            # Continue with normal processing
                            parser_output_list = data
                            logger.debug(f"[{request_id}] Processing {len(parser_output_list)} data records")
                        
                        elif response_type == "feedback":
                            # Publish feedback to command topic
                            logger.info(f"[{request_id}] Publishing feedback response to command topic for device {raw_message.device_id}")
                            await self._publish_command_feedback(raw_message.device_id, data, request_id)
                            return True  # Success, no further processing needed
                        
                        else:
                            logger.warning(f"[{request_id}] Unknown response_type '{response_type}' from action '{action}'. Discarding message.")
                            return True
                            
                    else:
                        logger.warning(f"[{request_id}] Unknown response_type '{response_type}' from action '{action}'. Discarding message.")
                        return True
                else:
                    logger.error(
                        f"[{request_id}] Script module does not have a '{action}' method."
                    )
                    await self._publish_processing_error(
                        f"Script Missing {action} Method",
                        error=f"Script module does not have a '{action}' method",
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
                
                device_datatypes = await self._get_device_datatypes(device, request_id)

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
                            labels=sensor_reading["labels"],
                            display_names=sensor_reading["display_names"],
                            index=sensor_reading["index"],
                            metadata={**sensor_reading["metadata"], **raw_message.metadata},
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
                        # Check if parser returned empty results (which is legitimate)
                        if len(parser_output_list) == 0:
                            # Parser successfully executed but produced no sensor readings
                            # This is legitimate for configuration messages, acknowledgments, etc.
                            logger.debug(f"[{request_id}] Parser successfully executed but produced no sensor readings for device {raw_message.device_id}")
                            return True
                        else:
                            # Parser produced data but validation failed to produce any valid outputs
                            # This suggests a validation issue
                            logger.error(f"[{request_id}] Parser produced {len(parser_output_list)} records but validation produced no valid outputs for device {raw_message.device_id}")
                            await self._publish_processing_error(
                                "Validation Failed to Produce Valid Outputs",
                                error=f"Parser produced {len(parser_output_list)} records but validation produced no valid outputs",
                                original_message=job_dict,
                                request_id=request_id,
                                details={"parser_output_count": len(parser_output_list)},
                                topic_details=(topic, partition, offset),
                            )
                            return True
                
                logger.debug(f"[{request_id}] Validated {len(validated_data_list)} validated outputs.")
                
                # 6.5 Process alarms for validated data
                try:
                    logger.debug(f"[{request_id}] Processing alarms for device {raw_message.device_id}")
                    if self.alarm_handler and validated_data_list:
                        logger.debug(f"[{request_id}] Alarm handler available and validated data list is not empty")
                        total_alerts_created = []
                        any_critical_status_alarm = False
                        # if raw_message.device_type == "light_controller":
                        # logger.info(f"[{request_id}] Validated data list: {validated_data_list}")
                        for validated_data in validated_data_list:
                            # Process alarms for each validated output
                            alarm_result = await self.alarm_handler.process_alarms(
                                sensor_id=str(device.id),
                                validated_data=validated_data,
                                script_module=script_module,
                                request_id=request_id
                            )
                            logger.debug(f"alarm result: {alarm_result}")
                            # Extract results from the returned dictionary
                            alert_ids = alarm_result.get("alert_ids", [])
                            triggered_alarms = alarm_result.get("triggered_alarms", [])
                            has_critical_status_alarm = alarm_result.get("has_critical_status_alarm", False)
                            
                            total_alerts_created.extend(alert_ids)
                            
                            # Track if any critical status alarm was triggered across all validated data
                            if has_critical_status_alarm:
                                any_critical_status_alarm = True
                        
                        # If any critical status alarm was triggered, set persist flags to False for ALL validated data
                        if any_critical_status_alarm:
                            logger.warning(f"[{request_id}] Critical status alarm triggered for device {raw_message.device_id} - setting persist flags to False for all validated data")
                            for validated_data in validated_data_list:
                                persist_flags = validated_data.metadata['persist']
                                validated_data.metadata['persist'] = [False] * len(persist_flags)
                                logger.debug(f"[{request_id}] Set {len(persist_flags)} persist flags to False for validated data")
                        
                        if total_alerts_created:
                            logger.info(f"[{request_id}] Created {len(total_alerts_created)} alerts for device {raw_message.device_id}")
                        else:
                            logger.debug(f"[{request_id}] No alarms triggered for device {raw_message.device_id}")
                    else:
                        logger.debug(f"[{request_id}] Alarm handler not available or no validated data to check")
                        
                except Exception as e:
                    logger.error(f"[{request_id}] Error processing alarms for device {raw_message.device_id}: {e}")
                    # Don't fail the entire message processing if alarm checking fails
                    # Log the error and continue with data publishing
                
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

            # 7. Publish Validated Data to Kafka using the wrapper
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

            except (KafkaException, Exception) as pub_err:
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
            logger.debug(f"[{request_id}] Found sensor with ID {sensor.id} for parameter {sensor.parameter}")
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
        Uses caching to reuse translator instances instead of creating new ones for each message.
        
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
            
            # Create cache key based on translator_used and protocol
            cache_key = f"{protocol}_{translator_used}"
            
            # Check if translator is already cached
            if cache_key in self.translator_cache:
                logger.debug(f"[{request_id}] Using cached translator '{translator_used}' for protocol '{protocol}'")
                return self.translator_cache[cache_key]
            
            logger.debug(f"[{request_id}] Initializing new translator '{translator_used}' for protocol '{protocol}'")
            
            # Determine connector ID based on protocol
            connector_id = f"{protocol}-connector"
            
            # Get translator configurations for the appropriate connector
            translator_configs = get_translator_configs(connector_id)
            
            if not translator_configs:
                logger.warning(f"[{request_id}] No translator configurations found for connector '{connector_id}'")
                return None
            
            # Find the matching translator configuration
            # All translators now use unique IDs based on configuration hash
            matching_config = None
            for config_item in translator_configs:
                translator_type = config_item.get('type')
                config_details = config_item.get('config', {})
                manufacturer = config_details.get('manufacturer', '')
                
                # Generate the expected translator ID for all translator types
                import hashlib
                import json
                config_str = json.dumps(config_details, sort_keys=True)
                config_hash = hashlib.md5(config_str.encode()).hexdigest()[:8]
                expected_translator_id = f"{manufacturer}_{config_hash}"
                
                # Unified pattern: {translator_type}_{translator_id}_
                expected_prefix = f"{translator_type}_{expected_translator_id}_"
                
                if translator_used.startswith(expected_prefix):
                    matching_config = config_item
                    break
            
            if not matching_config:
                logger.warning(f"[{request_id}] No matching translator config found for '{translator_used}'")
                return None
            
            # Create translator instance using the factory
            translator_instance = TranslatorFactory.create_translator(matching_config)
            
            if translator_instance:
                # Cache the translator instance for future use
                self.translator_cache[cache_key] = translator_instance
                logger.info(f"[{request_id}] Successfully initialized and cached translator '{translator_used}'")
                return translator_instance
            else:
                logger.error(f"[{request_id}] Failed to create translator instance for '{translator_used}'")
                return None
                
        except Exception as e:
            logger.exception(f"[{request_id}] Error initializing translator: {e}")
            return None

    def clear_translator_cache(self, cache_key: Optional[str] = None):
        """
        Clear translator cache - either specific translator or all cached translators.
        
        Args:
            cache_key: Optional specific cache key to clear. If None, clears all cached translators.
        """
        if cache_key:
            if cache_key in self.translator_cache:
                del self.translator_cache[cache_key]
                logger.info(f"Cleared cached translator: {cache_key}")
            else:
                logger.warning(f"Cache key not found: {cache_key}")
        else:
            cache_size = len(self.translator_cache)
            self.translator_cache.clear()
            logger.info(f"Cleared all cached translators (was {cache_size} translators)")

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
            # This might raise KafkaException or other exceptions if publishing fails
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
        """Main run loop: Uses AsyncResilientKafkaConsumer for automatic error handling."""
        self._running = True
        logger.info("Starting Normalizer Service...")

        try:
            # Initialize Kafka producer
            raw_producer = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)
            self.kafka_producer = NormalizerKafkaProducer(raw_producer)
            
            # Initialize admin user and alarm handler
            await self._initialize_alarm_handler()

            # Create resilient consumer with automatic error handling
            self.resilient_consumer = AsyncResilientKafkaConsumer(
                topic=config.KAFKA_RAW_DATA_TOPIC,
                group_id=config.KAFKA_CONSUMER_GROUP_ID,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                on_error_callback=self._on_consumer_error
            )
            
            logger.info(f"Consumer group '{config.KAFKA_CONSUMER_GROUP_ID}' starting for topic '{config.KAFKA_RAW_DATA_TOPIC}'...")
            
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_message,
                commit_offset=True,
                batch_processing=False  # Process messages individually
            )
            
        except Exception as e:
            logger.exception(f"Fatal error in Normalizer service: {e}")
            self._running = False
        finally:
            # Clean up
            if self.kafka_producer:
                try:
                    self.kafka_producer.close(timeout=10)
                except Exception as e:
                    logger.warning(f"Error closing Kafka producer: {e}")
            
            logger.info("Normalizer service stopped.")

    async def _on_consumer_error(self, error, context):
        """Handle consumer-level errors."""
        logger.error(f"Consumer error: {error}")
        # Could implement additional error handling logic here if needed

    async def stop(self):
        """Signals the service to stop gracefully."""
        if not self._running:
            logger.info("Stop already requested.")
            return
        logger.info("Stop requested for Normalizer Service.")
        
        self._running = False
        self._stop_event.set()
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()

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
                logger.debug(f"[{request_id}] No hardware found for sensor parameter {device_id}")
                return {}
                
            logger.debug(f"[{request_id}] Found hardware configuration for device {device_id}: {hardware.name}")
            return hardware.configuration
            
        except Exception as e:
            logger.error(f"[{request_id}] Error fetching hardware configuration for device {device_id}: {e}")
            return {}

    async def _publish_command_feedback(self, device_id: str, feedback_data: Dict[str, Any], request_id: Optional[str] = None):
        """
        Publish command feedback to the device commands topic.
        
        Args:
            device_id: The device ID to send feedback to
            feedback_data: The feedback data to send
            request_id: Optional request ID for logging
        """
        try:
            if not self.kafka_producer:
                logger.error(f"[{request_id}] Cannot publish command feedback - Kafka producer not available")
                return
                
            # Create CommandMessage from feedback data
            command_message = CommandMessage(
                device_id=feedback_data.get("device_id"),
                command_type="feedback",
                request_id=feedback_data.get("command_id"),
                payload=feedback_data.get("payload"),
                protocol=feedback_data.get("metadata").get("protocol"),  # Default to MQTT, could be made configurable
                metadata={
                    "source": "normalizer",
                    "request_id": request_id,
                    "timestamp": datetime.now().isoformat()
                }
            )
            
            # Publish to command topic using the kafka producer
            self.kafka_producer.publish_command(command_message)
            logger.info(f"[{request_id}] Published command feedback for device {device_id}")
            
        except Exception as e:
            logger.error(f"[{request_id}] Error publishing command feedback for device {device_id}: {e}")
            # Don't raise the exception as this is a secondary operation