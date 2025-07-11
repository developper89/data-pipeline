# normalizer/alarm_handler.py

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import uuid

from shared.models.common import ValidatedOutput, ErrorMessage, AlertMessage, AlarmMessage

# Import the alarm enums for direct comparison
from preservarium_sdk.domain.model.alarm import MathOperator, AlarmType, AlarmLevel
from preservarium_sdk.core.utils import translate

logger = logging.getLogger("alarm_handler")

class AlarmHandler:
    """
    Handles alarm checking and alert creation for incoming sensor data.
    Evaluates alarm conditions and creates alerts when thresholds are met.
    """
    
    def __init__(self, alarm_repository, alert_repository, kafka_producer=None):
        """
        Initialize the AlarmHandler with required repositories and optional Kafka producer.
        
        Args:
            alarm_repository: Repository for alarm CRUD operations
            alert_repository: Repository for alert CRUD operations 
            kafka_producer: Optional Kafka producer for publishing alerts/alarms
        """
        self.alarm_repository = alarm_repository
        self.alert_repository = alert_repository
        self.kafka_producer = kafka_producer
    
    def _get_language(self, request_id: Optional[str] = None) -> str:
        """
        Get the language for translations. Defaults to French as requested.
        
        Args:
            request_id: Optional request ID for tracing
            
        Returns:
            Language code (default: 'fr')
        """
        # TODO: In the future, this could be determined from user preferences or request headers
        return "fr"  # French as default language
    
    async def process_alarms(
        self, 
        sensor_id: str,
        validated_data: ValidatedOutput,
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process alarms for validated sensor data and create alerts if conditions are met.
        
        Args:
            sensor_id: The UUID of the sensor sending the data
            validated_data: ValidatedOutput object containing sensor measurements
            request_id: Optional request ID for tracing
            
        Returns:
            Dictionary containing:
            - alert_ids: List of alert IDs that were created
            - triggered_alarms: List of alarm objects that were triggered
            - has_critical_status_alarm: Boolean indicating if any critical status alarm was triggered
        """
        try:
            created_alert_ids = []
            triggered_alarms = []
            has_critical_status_alarm = False
            
            # Get datatype_id from validated data metadata
            data_datatype_id = validated_data.metadata['datatype_id']
            
            # Get active alarms for this sensor and datatype directly
            active_alarms = await self.alarm_repository.find_by(
                sensor_id=str(sensor_id),
                datatype_id=str(data_datatype_id),
                active=True
            )
            
            if not active_alarms:

                return {
                    "alert_ids": created_alert_ids,
                    "triggered_alarms": triggered_alarms,
                    "has_critical_status_alarm": has_critical_status_alarm
                }
            
            logger.debug(f"[{request_id}] Found {len(active_alarms)} active alarms for sensor {validated_data.device_id} and datatype {data_datatype_id}")
            
            # Check for unpublished alarms and publish them to Kafka
            # await self._publish_unpublished_alarms(active_alarms, validated_data.device_id, request_id)
            
            # Process each alarm
            for alarm in active_alarms:
                try:
                    # Check if this alarm should trigger based on the validated data
                    should_trigger, trigger_value = self._evaluate_alarm_condition(
                        alarm, validated_data, request_id
                    )
                    
                    if should_trigger:
                        # Track triggered alarm
                        triggered_alarms.append(alarm)
                        
                        # Check if this is a critical status alarm
                        if alarm.alarm_type == AlarmType.STATUS and alarm.level == AlarmLevel.CRITICAL:
                            has_critical_status_alarm = True
                            logger.warning(f"[{request_id}] Critical status alarm triggered: {alarm.name} - data will be published but not persisted")
                        
                        # Check if there's already an active alert (end_date is null) for this alarm
                        existing_active_alert = await self._get_active_alert_for_alarm(alarm.id, request_id)
                        
                        if existing_active_alert:
                            logger.debug(f"[{request_id}] Alarm condition still met for alarm {alarm.name}: active alert {existing_active_alert.id} continues")
                        else:
                            # No active alert exists, create new one
                            alert_id = await self._create_alert(
                                alarm, validated_data, trigger_value, request_id
                            )
                            if alert_id:
                                created_alert_ids.append(str(alert_id))
                                logger.info(f"[{request_id}] Created alert {alert_id} for alarm {alarm.name} on sensor {sensor_id}")
                    else:
                        # Condition not met - check if we need to close an active alert
                        existing_active_alert = await self._get_active_alert_for_alarm(alarm.id, request_id)
                        
                        if existing_active_alert:
                            await self._close_active_alert(existing_active_alert.id, validated_data.device_id, request_id)
                            logger.info(f"[{request_id}] Closed alert {existing_active_alert.id} for alarm {alarm.name}: condition no longer met")
                    
                except Exception as e:
                    logger.error(f"[{request_id}] Error processing alarm {alarm.id}: {e}")
                    continue
            
            return {
                "alert_ids": created_alert_ids,
                "triggered_alarms": triggered_alarms,
                "has_critical_status_alarm": has_critical_status_alarm
            }
            
        except Exception as e:
            logger.exception(f"[{request_id}] Error processing alarms for sensor {sensor_id}: {e}")
            return {
                "alert_ids": [],
                "triggered_alarms": [],
                "has_critical_status_alarm": False
            }
    
    def _evaluate_alarm_condition(
        self, 
        alarm: Any, 
        validated_data: ValidatedOutput,
        request_id: Optional[str] = None
    ) -> Tuple[bool, Optional[float]]:
        """
        Evaluate if an alarm condition is met based on validated data.
        
        Args:
            alarm: Alarm domain model object
            validated_data: ValidatedOutput containing sensor measurements
            request_id: Optional request ID for tracing
            
        Returns:
            Tuple of (should_trigger, trigger_value)
        """
        try:
            # Get the field value from validated data
            field_value = self._extract_field_value(alarm, validated_data)
            if field_value is None:
                logger.debug(f"[{request_id}] Field '{alarm.field_name}' not found in validated data")
                return False, None
            
            # Get the expected data type for this field
            expected_type = self._get_field_expected_type(alarm, validated_data, request_id)
            
            # Convert threshold to the expected type for comparison
            converted_threshold = self._convert_threshold_to_expected_type(alarm.threshold, expected_type)
            
            # Values are already converted by the validator, use them directly
            # Evaluate condition based on alarm type and math operator
            condition_met = self._evaluate_math_condition(
                field_value, alarm.math_operator, converted_threshold
            )
            
            if condition_met:
                logger.debug(f"[{request_id}] Alarm condition met: {field_value} {alarm.math_operator} {converted_threshold} (original: {alarm.threshold})")
                return True, field_value
            else:
                logger.debug(f"[{request_id}] Alarm condition not met: {field_value} {alarm.math_operator} {converted_threshold} (original: {alarm.threshold})")
                return False, field_value
            
        except Exception as e:
            logger.error(f"[{request_id}] Error evaluating alarm condition: {e}")
            return False, None
    
    def _extract_field_value(self, alarm: Any, validated_data: ValidatedOutput) -> Any:
        """
        Extract the field value from validated data based on alarm configuration.
        Uses field_label to match the correct value from the values array.
        
        Args:
            alarm: Alarm domain model object
            validated_data: ValidatedOutput containing measurements
            
        Returns:
            Field value or None if not found
        """
        field_label = alarm.field_label
        
        # Find the index of the field_label in the labels array
        try:
            field_index = validated_data.labels.index(field_label)
            return validated_data.values[field_index]
        except ValueError:
            logger.warning(f"Field label '{field_label}' not found in labels {validated_data.labels}")
            return None
        except IndexError:
            logger.warning(f"Field label '{field_label}' found at index {field_index} but values array has only {len(validated_data.values)} elements")
            return None
    
    def _get_field_expected_type(self, alarm: Any, validated_data: ValidatedOutput, request_id: Optional[str] = None) -> type:
        """
        Get the expected data type for a specific field from the ValidatedOutput metadata.
        
        Args:
            alarm: Alarm domain model object
            validated_data: ValidatedOutput containing metadata with expected types
            request_id: Optional request ID for tracing
            
        Returns:
            Python type (bool, float, str, etc.) for the field
        """
        try:
            # Get the field index from the labels array
            field_label = alarm.field_label
            field_index = validated_data.labels.index(field_label)
            
            # Get the expected types from metadata
            expected_types = validated_data.metadata.get('datatype_type', [])
            
            # Get the expected type for this field index
            if field_index < len(expected_types):
                expected_type_str = expected_types[field_index]
            elif expected_types:
                # Use the last available type if array is shorter
                expected_type_str = expected_types[-1]
            else:
                # Default to float if no type information available
                logger.warning(f"[{request_id}] No expected type found for field '{field_label}', defaulting to float")
                return float
            
            # Convert string representation to Python type
            type_mapping = {
                'boolean': bool,
                'number': float,
                'string': str,
                'datetime': str,  # Keep as string for now
                'list': list,
                'object': dict,
                'bytes': bytes
            }
            
            python_type = type_mapping.get(expected_type_str, float)

            return python_type
            
        except ValueError:
            logger.warning(f"[{request_id}] Field label '{field_label}' not found in labels {validated_data.labels}")
            return float  # Default to float
        except Exception as e:
            logger.error(f"[{request_id}] Error getting expected type for field '{alarm.field_label}': {e}")
            return float  # Default to float
    
    def _evaluate_math_condition(
        self, 
        value: Any, 
        operator: MathOperator, 
        threshold: Any
    ) -> bool:
        """
        Evaluate mathematical condition based on operator.
        
        Args:
            value: The measured value (any type)
            operator: MathOperator enum value
            threshold: Threshold value to compare against (already converted to expected type)
            
        Returns:
            True if condition is met, False otherwise
        """
        try:
            # Handle MathOperator enum
            if operator == MathOperator.GREATER_THAN:
                return value > threshold
            elif operator == MathOperator.LESS_THAN:
                return value < threshold
            elif operator == MathOperator.GREATER_THAN_OR_EQUAL:
                return value >= threshold
            elif operator == MathOperator.LESS_THAN_OR_EQUAL:
                return value <= threshold
            elif operator == MathOperator.EQUAL:
                return value == threshold
            elif operator == MathOperator.NOT_EQUAL:
                return value != threshold
            else:
                logger.warning(f"Unknown MathOperator enum: {operator}")
                return False
        except (TypeError, ValueError) as e:
            logger.warning(f"Cannot compare value '{value}' with threshold '{threshold}' using operator {operator}: {e}")
            return False
    
    def _convert_threshold_to_expected_type(self, threshold: Any, expected_type: type) -> Any:
        """
        Convert threshold value to the expected type.
        
        Args:
            threshold: The threshold value to convert
            expected_type: The expected Python type
            
        Returns:
            Converted threshold value
        """
        try:
            if expected_type == bool:
                # Convert numeric threshold to boolean
                if isinstance(threshold, (int, float)):
                    return bool(threshold)
                elif isinstance(threshold, str):
                    return threshold.lower() in ('true', 'yes', '1')
                else:
                    return bool(threshold)
            elif expected_type == float:
                return float(threshold)
            elif expected_type == str:
                return str(threshold)
            elif expected_type == int:
                return int(threshold)
            else:
                # For other types, try direct conversion
                return expected_type(threshold)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to convert threshold '{threshold}' to type {expected_type}: {e}")
            return threshold  # Return original threshold if conversion fails
    
    async def _create_alert(
        self,
        alarm: Any,
        validated_data: ValidatedOutput,
        trigger_value: float,
        request_id: Optional[str] = None
    ) -> Optional[uuid.UUID]:
        """
        Create an alert when an alarm condition is met and publish it to Kafka.
        
        Args:
            alarm: Alarm domain model object
            validated_data: ValidatedOutput that triggered the alarm
            trigger_value: The value that triggered the alarm
            request_id: Optional request ID for tracing
            
        Returns:
            UUID of created alert or None if creation failed
        """
        try:
            triggered_at = validated_data.timestamp or datetime.utcnow()
            alert_message = self._generate_alert_message(alarm, trigger_value, request_id)
            
            # Prepare alert data for database (matching API structure)
            alert_data = {
                "name": alarm.name,
                "message": alert_message,
                "treated": False,
                "start_date": triggered_at,
                "error_value": trigger_value,
                "alarm_id": str(alarm.id),
                "sensor_id": str(alarm.sensor_id),
                "mq_published_at": None  # Will be set after successful Kafka publishing
            }
            
            # Create alert using repository
            alert_id = await self.alert_repository.create(alert_data)
            
            logger.info(f"[{request_id}] Created alert {alert_id} for alarm '{alarm.name}' with value {trigger_value}")
            
            # Publish alert to Kafka if producer is available
            if self.kafka_producer and alert_id:
                try:
                    kafka_alert = AlertMessage(
                        request_id=request_id or str(uuid.uuid4()),
                        timestamp=triggered_at,
                        id=str(alert_id),
                        name=alarm.name,
                        message=alert_message,
                        treated=False,
                        start_date=triggered_at,
                        error_value=trigger_value,
                        is_resolved=False,  # This is a new alert, not resolved
                        # Additional message bus context fields
                        alarm_id=str(alarm.id),
                        sensor_id=str(alarm.sensor_id),
                        sensor_name=alarm.sensor_name,
                        device_id=validated_data.device_id,
                        alarm_type=alarm.alarm_type.value,  # Use .value to get "Measure" or "Status"
                        field_name=alarm.field_name,
                        threshold=alarm.threshold,
                        math_operator=alarm.math_operator.value,  # Use .value to get the symbol like ">"
                        level=alarm.level.name,  # Convert enum to string
                        recipients=getattr(alarm, 'recipients', None),
                        notify_creator=getattr(alarm, 'notify_creator', True),
                        alarm_creator_full_name=getattr(alarm, 'creator_full_name', None),
                        alarm_creator_email=getattr(alarm, 'creator_email', None)
                    )
                    
                    self.kafka_producer.publish_alert(kafka_alert)
                    logger.debug(f"[{request_id}] Published alert {alert_id} to Kafka")
                    
                    # Mark alert as published after successful Kafka publishing
                    await self._mark_alert_as_published(alert_id, request_id)
                    
                except Exception as kafka_error:
                    logger.error(f"[{request_id}] Failed to publish alert {alert_id} to Kafka: {kafka_error}")
                    # Don't fail the entire alert creation if Kafka publishing fails
            else:
                logger.debug(f"[{request_id}] Kafka producer not available, skipping alert publication")
            
            return alert_id
            
        except Exception as e:
            logger.error(f"[{request_id}] Failed to create alert for alarm {alarm.id}: {e}")
            return None
    
    def _translate_math_operator(self, operator: MathOperator, lang: str) -> str:
        """
        Translate math operator to human-readable text.
        
        Args:
            operator: MathOperator enum value
            lang: Language code
            
        Returns:
            Translated operator text
        """
        operator_map = {
            MathOperator.GREATER_THAN: "operator_greater_than",
            MathOperator.LESS_THAN: "operator_less_than", 
            MathOperator.GREATER_THAN_OR_EQUAL: "operator_greater_than_or_equal",
            MathOperator.LESS_THAN_OR_EQUAL: "operator_less_than_or_equal",
            MathOperator.EQUAL: "operator_equal",
            MathOperator.NOT_EQUAL: "operator_not_equal"
        }
        
        key = operator_map.get(operator, "operator_greater_than")
        return translate(key, lang, key)

    def _generate_alert_message(self, alarm: Any, trigger_value: float, request_id: Optional[str] = None) -> str:
        """
        Generate a human-readable alert message using translations.
        
        Args:
            alarm: Alarm domain model object
            trigger_value: The value that triggered the alarm
            request_id: Optional request ID for tracing
            
        Returns:
            Formatted alert message
        """
        lang = self._get_language(request_id)
        operator_text = self._translate_math_operator(alarm.math_operator, lang)
        
        return translate(
            "alert_triggered",
            lang,
            "alert_triggered",
            alarm_name=alarm.name,
            field_name=alarm.field_name,
            trigger_value=trigger_value,
            operator_text=operator_text,
            threshold=alarm.threshold
        )
    
    async def get_alarm_statistics(self, sensor_id: str) -> Dict[str, Any]:
        """
        Get statistics about alarms for a specific sensor.
        
        Args:
            sensor_id: The UUID of the sensor
            
        Returns:
            Dictionary containing alarm statistics
        """
        try:
            # Get all alarms for this sensor
            all_alarms = await self.alarm_repository.find_by(sensor_id=uuid.UUID(sensor_id))
            active_alarms = [alarm for alarm in all_alarms if alarm.active]
            
            # Count by alarm type
            status_alarms = len([alarm for alarm in active_alarms if alarm.alarm_type == "Status"])
            measure_alarms = len([alarm for alarm in active_alarms if alarm.alarm_type == "Measure"])
            
            return {
                "total_alarms": len(all_alarms),
                "active_alarms": len(active_alarms),
                "inactive_alarms": len(all_alarms) - len(active_alarms),
                "status_alarms": status_alarms,
                "measure_alarms": measure_alarms
            }
            
        except Exception as e:
            logger.error(f"Error getting alarm statistics for sensor {sensor_id}: {e}")
            return {
                "total_alarms": 0,
                "active_alarms": 0,
                "inactive_alarms": 0,
                "status_alarms": 0,
                "measure_alarms": 0,
                "error": str(e)
            }
    
    async def _publish_unpublished_alarms(
        self, 
        alarms: List[Any], 
        device_id: str, 
        request_id: Optional[str] = None
    ) -> None:
        """
        Check for alarms that haven't been published to Kafka and publish them.
        
        Args:
            alarms: List of alarm domain model objects
            device_id: Device ID for logging context
            request_id: Optional request ID for tracing
        """
        if not self.kafka_producer:
            logger.debug(f"[{request_id}] Kafka producer not available, skipping alarm publishing")
            return
        
        try:
            unpublished_count = 0
            
            for alarm in alarms:
                # Check if alarm has been published to message queue
                if not hasattr(alarm, 'mq_published_at') or alarm.mq_published_at is None:
                    try:
                        # Create and publish alarm message
                        await self._publish_alarm_configuration(alarm, device_id, request_id)
                        
                        # Mark alarm as published
                        await self._mark_alarm_as_published(alarm.id, request_id)
                        
                        unpublished_count += 1
                        logger.info(f"[{request_id}] Published alarm configuration '{alarm.name}' ({alarm.id}) to Kafka")
                        
                    except Exception as publish_error:
                        logger.error(f"[{request_id}] Failed to publish alarm {alarm.id} to Kafka: {publish_error}")
                        # Continue with other alarms even if one fails
                        continue
            
            if unpublished_count > 0:
                logger.info(f"[{request_id}] Published {unpublished_count} new alarm configurations for device {device_id}")
            else:
                logger.debug(f"[{request_id}] All alarms already published for device {device_id}")
                
        except Exception as e:
            logger.error(f"[{request_id}] Error checking/publishing unpublished alarms: {e}")
    
    async def _publish_alarm_configuration(
        self, 
        alarm: Any, 
        device_id: str, 
        request_id: Optional[str] = None
    ) -> None:
        """
        Publish an alarm configuration to Kafka.
        
        Args:
            alarm: Alarm domain model object
            device_id: Device ID parameter
            request_id: Optional request ID for tracing
        """
        try:
            kafka_alarm = AlarmMessage(
                request_id=request_id or str(uuid.uuid4()),
                timestamp=datetime.utcnow(),
                id=str(alarm.id),
                name=alarm.name,
                description=alarm.description,
                level=alarm.level.name,  # Convert enum to string
                math_operator=str(alarm.math_operator),
                active=alarm.active,
                threshold=alarm.threshold,
                field_name=alarm.field_name,
                field_label=getattr(alarm, 'field_label', alarm.field_name),
                datatype_id=str(alarm.datatype_id),
                sensor_id=str(alarm.sensor_id),
                alarm_type=str(alarm.alarm_type),
                user_id=str(alarm.user_id),
                recipients=getattr(alarm, 'recipients', None),
                notify_creator=getattr(alarm, 'notify_creator', True),
                creator_full_name=getattr(alarm, 'creator_full_name', None),
                creator_email=getattr(alarm, 'creator_email', None),
                # Additional message bus context fields
                device_id=device_id,
                created_at=getattr(alarm, 'created_at', datetime.utcnow()),
                updated_at=getattr(alarm, 'updated_at', datetime.utcnow())
            )
            
            self.kafka_producer.publish_alarm(kafka_alarm)
            logger.debug(f"[{request_id}] Successfully published alarm configuration {alarm.id} to Kafka")
            
        except Exception as e:
            logger.error(f"[{request_id}] Failed to create/publish alarm message for {alarm.id}: {e}")
            raise
    
    async def _mark_alarm_as_published(
        self, 
        alarm_id: uuid.UUID, 
        request_id: Optional[str] = None
    ) -> None:
        """
        Mark an alarm as published by updating the mq_published_at field.
        
        Args:
            alarm_id: UUID of the alarm to mark as published
            request_id: Optional request ID for tracing
        """
        try:
            # Update the alarm's mq_published_at field
            update_data = {
                "mq_published_at": datetime.utcnow()
            }
            
            await self.alarm_repository.update(alarm_id, update_data)
            logger.debug(f"[{request_id}] Marked alarm {alarm_id} as published to message queue")
            
        except Exception as e:
            logger.error(f"[{request_id}] Failed to mark alarm {alarm_id} as published: {e}")
            raise
    
    async def _mark_alert_as_published(
        self, 
        alert_id: uuid.UUID, 
        request_id: Optional[str] = None
    ) -> None:
        """
        Mark an alert as published by updating the mq_published_at field.
        
        Args:
            alert_id: UUID of the alert to mark as published
            request_id: Optional request ID for tracing
        """
        try:
            # Update the alert's mq_published_at field
            update_data = {
                "mq_published_at": datetime.utcnow()
            }
            
            await self.alert_repository.update(alert_id, update_data)
            logger.debug(f"[{request_id}] Marked alert {alert_id} as published to message queue")
            
        except Exception as e:
            logger.error(f"[{request_id}] Failed to mark alert {alert_id} as published: {e}")
            raise
    
    async def _get_active_alert_for_alarm(
        self, 
        alarm_id: uuid.UUID, 
        request_id: Optional[str] = None
    ) -> Optional[Any]:
        """
        Get the active alert (end_date is null) for a specific alarm.
        
        Args:
            alarm_id: UUID of the alarm to check for active alerts
            request_id: Optional request ID for tracing
            
        Returns:
            Active alert object or None if no active alert exists
        """
        try:
            # Find alerts for this alarm where end_date is null (active alerts)
            active_alerts = await self.alert_repository.find_by(
                alarm_id=alarm_id,
                treated=False,
                end_date=None  # This indicates an active alert
            )
            
            if active_alerts:
                # There should only be one active alert per alarm at a time
                if len(active_alerts) > 1:
                    logger.warning(f"[{request_id}] Found {len(active_alerts)} active alerts for alarm {alarm_id}, expected only 1")
                
                return active_alerts[0]  # Return the first (or only) active alert
            
            return None
            
        except Exception as e:
            logger.error(f"[{request_id}] Error finding active alert for alarm {alarm_id}: {e}")
            return None
    
    async def _close_active_alert(
        self, 
        alert_id: uuid.UUID, 
        device_id: str,
        request_id: Optional[str] = None
    ) -> None:
        """
        Close an active alert by setting its end_date and treated status.
        Also publishes resolution notification to Kafka for email alerts.
        
        Args:
            alert_id: UUID of the alert to close
            device_id: Device ID parameter
            request_id: Optional request ID for tracing
        """
        try:
            # Get the alert - it already contains all alarm information
            alert = await self.alert_repository.find(alert_id)
            if not alert:
                logger.error(f"[{request_id}] Alert {alert_id} not found for closing")
                return
            
            # Update the alert's end_date and treated status to mark it as resolved
            closed_at = datetime.utcnow()
            update_data = {
                "end_date": closed_at,
                "treated": True
            }
            
            await self.alert_repository.update(alert_id, update_data)
            logger.debug(f"[{request_id}] Closed alert {alert_id} by setting end_date and treated=True")
            
            # Publish resolution notification to Kafka if producer is available
            if self.kafka_producer:
                try:
                    # Create resolution alert message for email notification
                    lang = self._get_language(request_id)
                    resolution_message = translate(
                        "alert_resolved",
                        lang,
                        "alert_resolved",
                        alarm_name=alert.alarm_name or "Unknown"
                    )
                    
                    # Create translated alert name for the title
                    resolved_alert_name = translate(
                        "alert_resolved",
                        lang,
                        "alert_resolved",
                        alarm_name=alert.alarm_name or "Unknown"
                    )
                    
                    kafka_alert = AlertMessage(
                        request_id=request_id or str(uuid.uuid4()),
                        timestamp=closed_at,
                        id=str(alert_id),  # Use the same alert ID
                        name=resolved_alert_name,
                        message=resolution_message,
                        treated=True,
                        start_date=alert.start_date,
                        end_date=closed_at,
                        error_value=alert.error_value,
                        is_resolved=True,  # This is a resolved alert notification
                        # Additional message bus context fields
                        alarm_id=str(alert.alarm_id),
                        sensor_id=str(alert.sensor_id),
                        sensor_name=alert.sensor_name,
                        device_id=device_id,
                        alarm_type=alert.alarm_type.value if alert.alarm_type else "Unknown",  # Use .value
                        field_name=alert.alarm_field_name,
                        threshold=alert.alarm_threshold,
                        math_operator=alert.alarm_math_operator.value if alert.alarm_math_operator else "Unknown",  # Use .value
                        level=alert.alarm_level.name if alert.alarm_level else "INFO",
                        recipients=getattr(alert, 'recipients', None),
                        notify_creator=getattr(alert, 'notify_creator', True),
                        alarm_creator_full_name=alert.alarm_creator_full_name,
                        alarm_creator_email=alert.alarm_creator_email
                    )
                    
                    self.kafka_producer.publish_alert(kafka_alert)
                    logger.info(f"[{request_id}] Published resolution notification for alert {alert_id} to Kafka")
                    
                except Exception as kafka_error:
                    logger.error(f"[{request_id}] Failed to publish resolution notification for alert {alert_id} to Kafka: {kafka_error}")
                    # Don't fail the entire alert closure if Kafka publishing fails
            else:
                logger.debug(f"[{request_id}] Kafka producer not available, skipping resolution notification")
            
        except Exception as e:
            logger.error(f"[{request_id}] Failed to close alert {alert_id}: {e}")
            raise
    
    async def _get_sensor_for_alarm(self, alarm: Any) -> Any:
        """
        Helper method to get sensor information for an alarm.
        
        Args:
            alarm: Alarm domain model object
            
        Returns:
            Sensor object or None if not found
        """
        try:
            # This would need to be implemented based on your sensor repository
            # For now, returning None as placeholder
            logger.warning("_get_sensor_for_alarm not fully implemented - using sensor_id lookup")
            return None
        except Exception as e:
            logger.error(f"Error getting sensor for alarm {alarm.id}: {e}")
            return None 