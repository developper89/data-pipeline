# normalizer/alarm_handler.py

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import uuid

from shared.models.common import ValidatedOutput, ErrorMessage, AlertMessage, AlarmMessage

# Import the MathOperator enum for direct comparison
from preservarium_sdk.domain.model.alarm import MathOperator

logger = logging.getLogger(__name__)

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
            
            # Get all active alarms for this sensor
            active_alarms = await self.alarm_repository.find_by(
                sensor_id=uuid.UUID(sensor_id),
                active=True
            )
            
            if not active_alarms:
                logger.debug(f"[{request_id}] No active alarms found for sensor {sensor_id}")
                return created_alert_ids
            
            logger.debug(f"[{request_id}] Found {len(active_alarms)} active alarms for sensor {sensor_id}")
            
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
                        # Create alert
                        alert_id = await self._create_alert(
                            alarm, validated_data, trigger_value, request_id
                        )
                        if alert_id:
                            created_alert_ids.append(str(alert_id))
                            logger.info(f"[{request_id}] Created alert {alert_id} for alarm {alarm.name} on sensor {sensor_id}")
                    
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
            # Check if the alarm's datatype matches the validated data's datatype
            # Look for datatype_id in metadata
            data_datatype_id = None
            if validated_data.metadata and 'datatype_id' in validated_data.metadata:
                data_datatype_id = validated_data.metadata['datatype_id']
            
            if data_datatype_id and str(alarm.datatype_id) != str(data_datatype_id):
                logger.debug(f"[{request_id}] Alarm datatype {alarm.datatype_id} doesn't match data datatype {data_datatype_id}")
                return False, None
            
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
        
        Args:
            alarm: Alarm domain model object
            validated_data: ValidatedOutput containing measurements
            
        Returns:
            Field value or None if not found
        """
        field_name = alarm.field_name
        
        # Check if field exists in validated data values
        if hasattr(validated_data, 'values') and validated_data.values:
            # If values is a list, take the first value (most common case)
            if isinstance(validated_data.values, list) and validated_data.values:
                return validated_data.values[0]
            # If values is a dict, look for the specific field
            elif isinstance(validated_data.values, dict):
                return validated_data.values.get(field_name)
            # If values is a single value
            else:
                return validated_data.values
        
        # Check metadata if field not found in values
        if hasattr(validated_data, 'metadata') and validated_data.metadata:
            return validated_data.metadata.get(field_name)
        
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
            
            # Prepare alert data for database
            alert_data = {
                "alarm_id": alarm.id,
                "triggered_at": triggered_at,
                "value": trigger_value,
                "message": alert_message,
                "level": alarm.level,
                "resolved": False,
                "acknowledged": False
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
                        alert_id=str(alert_id),
                        alarm_id=str(alarm.id),
                        sensor_id=str(alarm.sensor_id),
                        device_id=validated_data.device_id,
                        alarm_name=alarm.name,
                        alarm_type=str(alarm.alarm_type),
                        field_name=alarm.field_name,
                        trigger_value=trigger_value,
                        threshold=alarm.threshold,
                        math_operator=str(alarm.math_operator),
                        level=alarm.level,
                        message=alert_message,
                        triggered_at=triggered_at,
                        recipients=getattr(alarm, 'recipients', None),
                        notify_creator=getattr(alarm, 'notify_creator', True)
                    )
                    
                    self.kafka_producer.publish_alert(kafka_alert)
                    logger.debug(f"[{request_id}] Published alert {alert_id} to Kafka")
                    
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
                alarm_id=str(alarm.id),
                sensor_id=str(alarm.sensor_id),
                device_id=device_id,
                datatype_id=str(alarm.datatype_id),
                alarm_name=alarm.name,
                description=alarm.description,
                alarm_type=str(alarm.alarm_type),
                field_name=alarm.field_name,
                threshold=alarm.threshold,
                math_operator=str(alarm.math_operator),
                level=alarm.level,
                active=alarm.active,
                user_id=str(alarm.user_id),
                recipients=getattr(alarm, 'recipients', None),
                notify_creator=getattr(alarm, 'notify_creator', True),
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