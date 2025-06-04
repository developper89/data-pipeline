# coap_connector/command_consumer.py
import logging
import asyncio
import json
from typing import Dict, Any, Optional, List
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import create_kafka_consumer
from . import config

logger = logging.getLogger(__name__)

class CommandConsumer:
    """
    Consumes device command messages from Kafka and processes them for CoAP devices.
    In CoAP's case, commands are stored for later fetching when devices poll the server,
    as CoAP is often used with sleeping devices that can't receive push messages directly.
    
    Commands are expected to be pre-formatted by the pusher-api before being published to Kafka.
    """
    
    def __init__(self):
        """
        Initialize the command consumer.
        """
        self.consumer = None
        self.running = False
        self._stop_event = asyncio.Event()
        self._task = None
        
        # In-memory store for pending commands, indexed by device_id
        # In a production system, this would be a persistent store
        self.pending_commands: Dict[str, List[Dict[str, Any]]] = {}
        
    async def start(self):
        """Start the command consumer."""
        if self.running:
            logger.warning("Command consumer already running")
            return
            
        self.running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("Command consumer started")
        
    async def stop(self):
        """Stop the command consumer."""
        if not self.running:
            logger.warning("Command consumer not running")
            return
            
        logger.info("Stopping command consumer...")
        self.running = False
        self._stop_event.set()
        
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Command consumer task did not finish promptly, cancelling")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
            
        await self._close_consumer()
        logger.info("Command consumer stopped")
            
    async def _consume_loop(self):
        """Main loop for consuming command messages."""
        retry_delay = 5  # Initial retry delay in seconds
        max_retry_delay = 60  # Maximum retry delay in seconds
        
        while self.running:
            try:
                # Create the consumer if it doesn't exist
                if not self.consumer:
                    self.consumer = create_kafka_consumer(
                        topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
                        group_id=f"coap_command_consumer",
                        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                        auto_offset_reset='latest'  # Only consume new messages
                    )
                    if not self.consumer:
                        logger.error("Failed to create Kafka consumer, will retry...")
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
                            
                        await self._process_command(message.value)
                
                # Commit offsets
                self.consumer.commit()
                    
            except KafkaError as e:
                logger.error(f"Kafka error in command consumer: {e}")
                await self._close_consumer()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                logger.exception(f"Unexpected error in command consumer: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                
            # Check if we should stop
            if self._stop_event.is_set():
                break
    
    async def _process_command(self, command_data: Dict[str, Any]):
        """
        Process a command message from Kafka.
        Store the command for the device to retrieve later.
        
        Args:
            command_data: The command data from Kafka
        """
        try:
            # Check if this is a CoAP command
            if command_data.get('protocol', '').lower() != 'coap':
                logger.debug(f"Ignoring non-CoAP command: {command_data.get('protocol')}")
                return
                
            # Extract required fields
            device_id = command_data.get('device_id')
            request_id = command_data.get('request_id', 'unknown')
            
            if not device_id:
                logger.error(f"[{request_id}] Invalid command message: missing device_id")
                return
                
            # Store the command in the pending commands store
            if device_id not in self.pending_commands:
                self.pending_commands[device_id] = []
                
            # Add timestamp to track command age
            command_data['stored_at'] = time.time()
            
            # Add to pending commands
            self.pending_commands[device_id].append(command_data)
            logger.info(f"[{request_id}] Stored command for device {device_id}")
                
        except Exception as e:
            logger.exception(f"Error processing command: {e}")
    
    async def _close_consumer(self):
        """Close the Kafka consumer if it exists."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka command consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None
                
    def get_pending_commands(self, device_id: str) -> List[Dict[str, Any]]:
        """
        Get all pending commands for a device.
        
        Args:
            device_id: The device ID to get commands for
            
        Returns:
            List of command dictionaries
        """
        return self.pending_commands.get(device_id, [])
    
    def acknowledge_command(self, device_id: str, command_id: str) -> bool:
        """
        Acknowledge that a command has been delivered to a device.
        Removes the command from the pending list.
        
        Args:
            device_id: The device ID the command was for
            command_id: The unique identifier of the command (request_id)
            
        Returns:
            True if the command was found and removed, False otherwise
        """
        if device_id not in self.pending_commands:
            return False
            
        # Find the command by request_id
        for i, command in enumerate(self.pending_commands[device_id]):
            if command.get('request_id') == command_id:
                # Remove it from the list
                self.pending_commands[device_id].pop(i)
                logger.info(f"Command {command_id} for device {device_id} acknowledged")
                return True
                
        return False
    
    async def get_formatted_command(self, device_id: str) -> Optional[bytes]:
        """
        Get a pre-formatted command for a device.
        Commands must be pre-formatted by the pusher-api.
        
        Args:
            device_id: The device ID to get a command for
            
        Returns:
            Pre-formatted command bytes or None if no commands available
        """
        if not self.pending_commands.get(device_id):
            return None
            
        # Get the oldest command
        command = self.pending_commands[device_id][0]
        command_id = command.get('request_id', 'unknown')
        
        try:
            # Check if command is pre-formatted (from pusher-api)
            payload = command.get('payload', {})
            if isinstance(payload, dict) and 'formatted_command' in payload:
                # Command was pre-formatted by pusher-api
                formatted_hex = payload['formatted_command']
                try:
                    formatted_command = bytes.fromhex(formatted_hex)
                    logger.info(f"[{command_id}] Using pre-formatted command for device {device_id}")
                    return formatted_command
                except ValueError as e:
                    logger.error(f"[{command_id}] Invalid hex string in pre-formatted command: {e}")
                    return None
            else:
                # Command is not pre-formatted - this should not happen with the new architecture
                logger.error(f"[{command_id}] Command not pre-formatted for device {device_id}. All commands must be formatted by pusher-api.")
                return None
                
        except Exception as e:
            logger.exception(f"[{command_id}] Error getting pre-formatted command for device {device_id}: {e}")
            return None 