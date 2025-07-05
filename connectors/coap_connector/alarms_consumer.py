# coap_connector/alarms_consumer.py
import logging
import asyncio
import json
from typing import Dict, Any, Optional, List
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import create_kafka_consumer
import config

logger = logging.getLogger(__name__)

class AlarmsConsumer:
    """
    Consumes alarm configuration messages from Kafka and stores them for CoAP devices.
    This allows the CoAP connector to have up-to-date alarm configurations for all
    devices it manages, enabling local alarm evaluation or device-specific responses.
    
    Alarm configurations are published when alarms are discovered or updated.
    """
    
    def __init__(self):
        """
        Initialize the alarms consumer.
        """
        self.consumer = None
        self.running = False
        self._stop_event = asyncio.Event()
        self._task = None
        
        # In-memory store for alarm configurations, indexed by device_id
        # Structure: {device_id: [alarm_config1, alarm_config2, ...]}
        self.device_alarms: Dict[str, List[Dict[str, Any]]] = {}
        
        # Index alarms by alarm_id for quick lookups
        # Structure: {alarm_id: alarm_config}
        self.alarms_by_id: Dict[str, Dict[str, Any]] = {}
        
    async def start(self):
        """Start the alarms consumer."""
        if self.running:
            logger.warning("Alarms consumer already running")
            return
            
        self.running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("Alarms consumer started")
        
    async def stop(self):
        """Stop the alarms consumer."""
        if not self.running:
            logger.warning("Alarms consumer not running")
            return
            
        logger.info("Stopping alarms consumer...")
        self.running = False
        self._stop_event.set()
        
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Alarms consumer task did not finish promptly, cancelling")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
            
        await self._close_consumer()
        logger.info("Alarms consumer stopped")
            
    async def _consume_loop(self):
        """Main loop for consuming alarm configuration messages."""
        retry_delay = 5  # Initial retry delay in seconds
        max_retry_delay = 60  # Maximum retry delay in seconds
        
        while self.running:
            try:
                # Create the consumer if it doesn't exist
                if not self.consumer:
                    self.consumer = create_kafka_consumer(
                        topic=config.KAFKA_ALARMS_TOPIC,
                        group_id=f"coap_alarms_consumer",
                        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                        auto_offset_reset='earliest'  # Get all alarm configs from beginning
                    )
                    if not self.consumer:
                        logger.error("Failed to create Kafka consumer for alarms, will retry...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff
                        continue
                        
                    retry_delay = 5  # Reset retry delay on successful connection
                
                # Poll for messages (non-blocking in asyncio context)
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    # No messages, check if we should stop
                    if self._stop_event.is_set():
                        break
                    await asyncio.sleep(0.1)
                    continue
                
                # Process received messages
                for tp, messages in message_batch.items():
                    for message in messages:
                        if self._stop_event.is_set():
                            break
                            
                        await self._process_alarm_config(message.value)
                
                # Commit offsets
                self.consumer.commit()
                    
            except KafkaError as e:
                logger.error(f"Kafka error in alarms consumer: {e}")
                await self._close_consumer()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                logger.exception(f"Unexpected error in alarms consumer: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                
            # Check if we should stop
            if self._stop_event.is_set():
                break
    
    async def _process_alarm_config(self, alarm_data: Dict[str, Any]):
        """
        Process an alarm configuration message from Kafka.
        Store or update the alarm configuration for the device.
        
        Args:
            alarm_data: The alarm configuration data from Kafka
        """
        try:
            # Extract required fields
            alarm_id = alarm_data.get('alarm_id')
            device_id = alarm_data.get('device_id')
            request_id = alarm_data.get('request_id', 'unknown')
            
            if not alarm_id or not device_id:
                logger.error(f"[{request_id}] Invalid alarm message: missing alarm_id or device_id")
                return
            
            # Add processing timestamp
            alarm_data['received_at'] = time.time()
            
            # Store in both indexes
            self.alarms_by_id[alarm_id] = alarm_data
            
            # Initialize device alarm list if it doesn't exist
            if device_id not in self.device_alarms:
                self.device_alarms[device_id] = []
                
            # Check if this alarm already exists for the device (update case)
            existing_index = None
            for i, existing_alarm in enumerate(self.device_alarms[device_id]):
                if existing_alarm.get('alarm_id') == alarm_id:
                    existing_index = i
                    break
            
            if existing_index is not None:
                # Update existing alarm
                self.device_alarms[device_id][existing_index] = alarm_data
                logger.info(f"[{request_id}] Updated alarm config {alarm_id} for device {device_id}")
            else:
                # Add new alarm
                self.device_alarms[device_id].append(alarm_data)
                logger.info(f"[{request_id}] Added new alarm config {alarm_id} for device {device_id}")
            
            logger.debug(f"[{request_id}] Device {device_id} now has {len(self.device_alarms[device_id])} alarms")
                
        except Exception as e:
            logger.exception(f"Error processing alarm configuration: {e}")
    
    async def _close_consumer(self):
        """Close the Kafka consumer if it exists."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka alarms consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka alarms consumer: {e}")
            finally:
                self.consumer = None
                
    def get_device_alarms(self, device_id: str) -> List[Dict[str, Any]]:
        """
        Get all alarm configurations for a specific device.
        
        Args:
            device_id: The device ID to get alarms for
            
        Returns:
            List of alarm configuration dictionaries
        """
        return self.device_alarms.get(device_id, [])
    
    def get_active_device_alarms(self, device_id: str) -> List[Dict[str, Any]]:
        """
        Get only active alarm configurations for a specific device.
        
        Args:
            device_id: The device ID to get active alarms for
            
        Returns:
            List of active alarm configuration dictionaries
        """
        all_alarms = self.device_alarms.get(device_id, [])
        return [alarm for alarm in all_alarms if alarm.get('active', False)]
    
    def get_alarm_by_id(self, alarm_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific alarm configuration by its ID.
        
        Args:
            alarm_id: The alarm ID to lookup
            
        Returns:
            Alarm configuration dictionary or None if not found
        """
        return self.alarms_by_id.get(alarm_id)
    
    def get_alarms_by_type(self, device_id: str, alarm_type: str) -> List[Dict[str, Any]]:
        """
        Get alarm configurations for a device filtered by alarm type.
        
        Args:
            device_id: The device ID to get alarms for
            alarm_type: The alarm type to filter by ('Status' or 'Measure')
            
        Returns:
            List of alarm configurations matching the type
        """
        device_alarms = self.device_alarms.get(device_id, [])
        return [alarm for alarm in device_alarms if alarm.get('alarm_type') == alarm_type]
    
    def get_alarms_by_datatype(self, device_id: str, datatype_id: str) -> List[Dict[str, Any]]:
        """
        Get alarm configurations for a device filtered by datatype.
        
        Args:
            device_id: The device ID to get alarms for
            datatype_id: The datatype ID to filter by
            
        Returns:
            List of alarm configurations for the specified datatype
        """
        device_alarms = self.device_alarms.get(device_id, [])
        return [alarm for alarm in device_alarms if alarm.get('datatype_id') == datatype_id]
    
    def get_alarm_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about stored alarm configurations.
        
        Returns:
            Dictionary containing alarm statistics
        """
        total_alarms = len(self.alarms_by_id)
        total_devices = len(self.device_alarms)
        
        active_alarms = sum(1 for alarm in self.alarms_by_id.values() if alarm.get('active', False))
        
        # Count by alarm type
        status_alarms = sum(1 for alarm in self.alarms_by_id.values() if alarm.get('alarm_type') == 'Status')
        measure_alarms = sum(1 for alarm in self.alarms_by_id.values() if alarm.get('alarm_type') == 'Measure')
        
        # Devices with alarms
        devices_with_alarms = len([d for d in self.device_alarms if self.device_alarms[d]])
        
        return {
            "total_alarms": total_alarms,
            "active_alarms": active_alarms,
            "inactive_alarms": total_alarms - active_alarms,
            "status_alarms": status_alarms,
            "measure_alarms": measure_alarms,
            "total_devices_tracked": total_devices,
            "devices_with_alarms": devices_with_alarms,
            "average_alarms_per_device": round(total_alarms / max(devices_with_alarms, 1), 2)
        }
    
    def remove_alarm(self, alarm_id: str) -> bool:
        """
        Remove an alarm configuration (typically when alarm is deleted).
        
        Args:
            alarm_id: The alarm ID to remove
            
        Returns:
            True if the alarm was found and removed, False otherwise
        """
        # Find and remove from alarm ID index
        alarm_config = self.alarms_by_id.pop(alarm_id, None)
        if not alarm_config:
            return False
        
        # Find and remove from device index
        device_id = alarm_config.get('device_id')
        if device_id and device_id in self.device_alarms:
            self.device_alarms[device_id] = [
                alarm for alarm in self.device_alarms[device_id] 
                if alarm.get('alarm_id') != alarm_id
            ]
            
            # Clean up empty device entries
            if not self.device_alarms[device_id]:
                del self.device_alarms[device_id]
        
        logger.info(f"Removed alarm configuration {alarm_id} for device {device_id}")
        return True
    
    def clear_device_alarms(self, device_id: str) -> int:
        """
        Clear all alarms for a specific device.
        
        Args:
            device_id: The device ID to clear alarms for
            
        Returns:
            Number of alarms that were removed
        """
        if device_id not in self.device_alarms:
            return 0
        
        device_alarms = self.device_alarms[device_id]
        count = len(device_alarms)
        
        # Remove from alarm ID index
        for alarm in device_alarms:
            alarm_id = alarm.get('alarm_id')
            if alarm_id:
                self.alarms_by_id.pop(alarm_id, None)
        
        # Clear device alarms
        del self.device_alarms[device_id]
        
        logger.info(f"Cleared {count} alarm configurations for device {device_id}")
        return count 