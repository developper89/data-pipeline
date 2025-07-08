# normalizer/alarm_handler.py

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import uuid

from shared.models.common import ValidatedOutput, ErrorMessage, AlertMessage, AlarmMessage

# Import the MathOperator enum for direct comparison
from preservarium_sdk.domain.model.alarm import MathOperator

logger = logging.getLogger("alarm_handler")

class AlarmHandler:
    """
    Handles alarm checking and alert creation for incoming sensor data.
    Evaluates alarm conditions and creates alerts when thresholds are met.
    """
    
    def __init__(self, alarm_repository, alert_repository, kafka_producer=None):
        """
        Initialize the alarm handler with repositories for accessing alarms and creating alerts.
        
        Args:
            alarm_repository: Repository for accessing alarm configurations
            alert_repository: Repository for creating and managing alerts
            kafka_producer: Optional Kafka producer for publishing alerts
        """
        self.alarm_repository = alarm_repository
        self.alert_repository = alert_repository
        self.kafka_producer = kafka_producer
        
    async def process_alarms(
        self, 
        sensor_id: str,
        validated_data: ValidatedOutput,
        request_id: Optional[str] = None
    ) -> List[str]:
        """
        Process alarms for validated sensor data and create alerts if conditions are met.
        
        Args:
            sensor_id: The UUID of the sensor sending the data
            validated_data: ValidatedOutput object containing sensor measurements
            request_id: Optional request ID for tracing
            
        Returns:
            List of alert IDs that were created
        """
        try:
            created_alert_ids = []
            
            # Get datatype_id from validated data metadata
            data_datatype_id = validated_data.metadata['datatype_id']
            
            # Get active alarms for this sensor and datatype directly
            active_alarms = await self.alarm_repository.find_by(
                sensor_id=str(sensor_id),
                datatype_id=str(data_datatype_id),
                active=True
            )
            
            if not active_alarms:
                logger.debug(f"[{request_id}] No active alarms found for sensor {validated_data.device_id} and datatype {data_datatype_id}")
                return created_alert_ids
            
            logger.debug(f"[{request_id}] Found {len(active_alarms)} active alarms for sensor {validated_data.device_id} and datatype {data_datatype_id}")
            
            # Check for unpublished alarms and publish them to Kafka
            await self._publish_unpublished_alarms(active_alarms, validated_data.device_id, request_id)
            
            # Process each alarm
            for alarm in active_alarms:
                try:
                    # Check if this alarm should trigger based on the validated data
                    should_trigger, trigger_value = self._evaluate_alarm_condition(
                        alarm, validated_data, request_id
                    )
                    
                    if should_trigger:
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
                            await self._close_active_alert(existing_active_alert.id, request_id)
                            logger.info(f"[{request_id}] Closed alert {existing_active_alert.id} for alarm {alarm.name}: condition no longer met")
                    
                except Exception as e:
                    logger.error(f"[{request_id}] Error processing alarm {alarm.id}: {e}")
                    continue
            
            return created_alert_ids
            
        except Exception as e:
            logger.exception(f"[{request_id}] Error processing alarms for sensor {sensor_id}: {e}")
            return []
    
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
            
            # Values are already converted by the validator, use them directly
            # Evaluate condition based on alarm type and math operator
            condition_met = self._evaluate_math_condition(
                field_value, alarm.math_operator, alarm.threshold
            )
            
            if condition_met:
                logger.debug(f"[{request_id}] Alarm condition met: {field_value} {alarm.math_operator} {alarm.threshold}")
                return True, field_value
            else:
                logger.debug(f"[{request_id}] Alarm condition not met: {field_value} {alarm.math_operator} {alarm.threshold}")
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
            threshold: Threshold value to compare against (any type)
            
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
            alert_message = self._generate_alert_message(alarm, trigger_value)
            
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
                        # Additional message bus context fields
                        alarm_id=str(alarm.id),
                        sensor_id=str(alarm.sensor_id),
                        device_id=validated_data.device_id,
                        alarm_type=str(alarm.alarm_type),
                        field_name=alarm.field_name,
                        threshold=alarm.threshold,
                        math_operator=str(alarm.math_operator),
                        level=alarm.level,
                        recipients=getattr(alarm, 'recipients', None),
                        notify_creator=getattr(alarm, 'notify_creator', True)
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
    
    def _generate_alert_message(self, alarm: Any, trigger_value: float) -> str:
        """
        Generate a human-readable alert message.
        
        Args:
            alarm: Alarm domain model object
            trigger_value: The value that triggered the alarm
            
        Returns:
            Formatted alert message
        """
        # Handle MathOperator enum directly
        if alarm.math_operator == MathOperator.GREATER_THAN:
            operator_text = "exceeded"
        elif alarm.math_operator == MathOperator.LESS_THAN:
            operator_text = "fell below"
        elif alarm.math_operator == MathOperator.GREATER_THAN_OR_EQUAL:
            operator_text = "reached or exceeded"
        elif alarm.math_operator == MathOperator.LESS_THAN_OR_EQUAL:
            operator_text = "reached or fell below"
        elif alarm.math_operator == MathOperator.EQUAL:
            operator_text = "equals"
        elif alarm.math_operator == MathOperator.NOT_EQUAL:
            operator_text = "does not equal"
        else:
            operator_text = "triggered condition for"
        
        return (f"Alarm '{alarm.name}': {alarm.field_name} value {trigger_value} "
                f"{operator_text} threshold {alarm.threshold}")
    
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
                level=alarm.level,
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
        request_id: Optional[str] = None
    ) -> None:
        """
        Close an active alert by setting its end_date.
        
        Args:
            alert_id: UUID of the alert to close
            request_id: Optional request ID for tracing
        """
        try:
            # Update the alert's end_date to mark it as resolved
            update_data = {
                "end_date": datetime.utcnow()
            }
            
            await self.alert_repository.update(alert_id, update_data)
            logger.debug(f"[{request_id}] Closed alert {alert_id} by setting end_date")
            
        except Exception as e:
            logger.error(f"[{request_id}] Failed to close alert {alert_id}: {e}")
            raise
    
    async def republish_all_unpublished_alarms(self, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Utility method to republish all unpublished alarms across all sensors.
        Useful for recovery scenarios or initial synchronization.
        
        Args:
            request_id: Optional request ID for tracing
            
        Returns:
            Dictionary with republishing statistics
        """
        if not self.kafka_producer:
            logger.warning(f"[{request_id}] Kafka producer not available, cannot republish alarms")
            return {"error": "Kafka producer not available"}
        
        try:
            # Find all unpublished alarms
            all_alarms = await self.alarm_repository.find_by(active=True)
            unpublished_alarms = [
                alarm for alarm in all_alarms 
                if not hasattr(alarm, 'mq_published_at') or alarm.mq_published_at is None
            ]
            
            if not unpublished_alarms:
                logger.info(f"[{request_id}] No unpublished alarms found")
                return {
                    "total_alarms": len(all_alarms),
                    "unpublished_alarms": 0,
                    "published_count": 0,
                    "failed_count": 0
                }
            
            published_count = 0
            failed_count = 0
            
            logger.info(f"[{request_id}] Found {len(unpublished_alarms)} unpublished alarms to republish")
            
            for alarm in unpublished_alarms:
                try:
                    # Get sensor to find device_id
                    sensor = await self._get_sensor_for_alarm(alarm)
                    device_id = sensor.parameter if sensor else "unknown"
                    
                    # Publish alarm configuration
                    await self._publish_alarm_configuration(alarm, device_id, request_id)
                    
                    # Mark as published
                    await self._mark_alarm_as_published(alarm.id, request_id)
                    
                    published_count += 1
                    logger.debug(f"[{request_id}] Republished alarm '{alarm.name}' ({alarm.id})")
                    
                except Exception as e:
                    failed_count += 1
                    logger.error(f"[{request_id}] Failed to republish alarm {alarm.id}: {e}")
                    continue
            
            result = {
                "total_alarms": len(all_alarms),
                "unpublished_alarms": len(unpublished_alarms),
                "published_count": published_count,
                "failed_count": failed_count
            }
            
            logger.info(f"[{request_id}] Republishing complete: {published_count} published, {failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"[{request_id}] Error during alarm republishing: {e}")
            return {"error": str(e)}
    
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