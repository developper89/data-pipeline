# coap_connector/command_consumer.py
import logging
import asyncio
import json
import os
from typing import Dict, Any, Optional, List
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import create_kafka_consumer
from shared.utils.script_client import ScriptClient
from shared.config_loader import get_translator_configs
from shared.translation.factory import TranslatorFactory
import config

logger = logging.getLogger(__name__)

class CommandConsumer:
    """
    Enhanced command consumer that uses parser-based command formatting.
    All commands are formatted using device-specific parsers.
    """
    
    def __init__(self):
        """
        Initialize the command consumer with parser support.
        """
        self.consumer = None
        self.running = False
        self._stop_event = asyncio.Event()
        self._task = None
        
        # In-memory store for pending commands, indexed by device_id
        # In a production system, this would be a persistent store
        self.pending_commands: Dict[str, List[Dict[str, Any]]] = {}
        
        # Parser-based command formatting support
        self.script_client = ScriptClient(
            storage_type=config.SCRIPT_STORAGE_TYPE,
            local_dir=config.LOCAL_SCRIPT_DIR
        )
        # Cache for loaded parsers and translators
        self._parser_cache = {}
        self._translator_cache = {}
        logger.info("Command consumer initialized with parser-based formatting support")
        
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
        Process command using parser-based formatting.

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

            # Format command using parser-based approach
            formatted_command = await self._format_command_with_parser(device_id, command_data)
            if formatted_command:
                # Store the formatted command
                await self._store_formatted_command(device_id, command_data, formatted_command)
                logger.info(f"[{request_id}] Formatted and stored command for device {device_id}")
                return

            # If we get here, we couldn't format the command
            logger.error(f"[{request_id}] Unable to format command for device {device_id}: parser script not found or formatting failed")

        except Exception as e:
            logger.exception(f"Error processing command: {e}")

    async def _store_formatted_command(self, device_id: str, command_data: Dict[str, Any], formatted_command: bytes):
        """Store a command formatted by parser."""
        # Convert formatted binary command to hex string for storage
        hex_command = formatted_command.hex()

        # Update command data with formatted payload
        enhanced_command = command_data.copy()
        enhanced_command['payload'] = {
            'formatted_command': hex_command,
            'formatted_by': 'parser',
            'original_payload': command_data.get('payload', {})
        }
        enhanced_command['stored_at'] = time.time()

        # Store the enhanced command
        if device_id not in self.pending_commands:
            self.pending_commands[device_id] = []
        self.pending_commands[device_id].append(enhanced_command)

    async def _format_command_with_parser(self, device_id: str, command_data: dict) -> Optional[bytes]:
        """
        Format command using device-specific parser.
        
        Args:
            device_id: Target device identifier
            command_data: Command data dictionary from CommandMessage:
                - device_id: Target device identifier
                - command_type: Type of command (e.g., "alarm", "config")
                - payload: Command-specific data
                - protocol: Communication protocol
                - metadata: Optional metadata containing parser_script_path and manufacturer
            
        Returns:
            bytes: Formatted binary command or None if formatting fails
        """
        try:
            # Get parser script path and manufacturer from command metadata
            metadata = command_data.get('metadata', {})
            parser_script_path = metadata.get('parser_script_path')
            if not parser_script_path:
                logger.error(f"No parser script path provided in command metadata for device {device_id}")
                return None
            
            manufacturer = metadata.get('manufacturer')
            if not manufacturer:
                logger.warning(f"No manufacturer provided in command metadata for device {device_id}, translator selection may be less accurate")

            # Load parser module (with caching)
            script_module = await self._get_parser_module(parser_script_path)
            if not script_module:
                logger.error(f"Failed to load parser for device {device_id} from path {parser_script_path}")
                return None

            # Check if parser supports command formatting
            if not hasattr(script_module, 'format_command'):
                logger.error(f"Parser for device {device_id} does not support command formatting")
                return None

            # Get translator instance based on command type and manufacturer
            command_type = command_data.get('command_type')
            manufacturer = command_data.get('manufacturer')
            translator = await self._get_translator_for_device(device_id, command_type, manufacturer)

            # Use empty hardware configuration for now
            hardware_config = {}

            # Format command using parser
            binary_command = script_module.format_command(
                command=command_data,
                translator=translator,
                config=hardware_config
            )

            return binary_command

        except Exception as e:
            logger.exception(f"Error formatting command for device {device_id}: {e}")
            return None

    async def _get_parser_module(self, file_path: str):
        """Get parser module with caching."""
        if file_path not in self._parser_cache:
            try:
                # If file_path is relative, use it directly; if absolute, get basename
                if os.path.isabs(file_path):
                    filename = os.path.basename(file_path)
                else:
                    filename = file_path
                
                script_ref = os.path.join(self.script_client.local_dir, filename)
                module = await self.script_client.get_module(script_ref)
                self._parser_cache[file_path] = module
            except Exception as e:
                logger.error(f"Failed to load parser module {file_path}: {e}")
                return None

        return self._parser_cache[file_path]

    async def _get_translator_for_device(self, device_id: str, command_type: str = None, manufacturer: str = None):
        """Get translator instance for device based on command type and manufacturer."""
        cache_key = f"{manufacturer or 'default'}_{command_type or 'default'}_translator"

        if cache_key not in self._translator_cache:
            try:
                # Get translator configs for coap-connector
                translator_configs = get_translator_configs("coap-connector")

                # Find appropriate translator based on manufacturer and command type
                selected_translator_config = None
                
                for translator_config in translator_configs:
                    config_details = translator_config.get('config', {})
                    
                    # Check manufacturer match first (most important)
                    config_manufacturer = config_details.get('manufacturer', '')
                    if manufacturer and config_manufacturer != manufacturer:
                        continue
                    
                    # Check if this translator supports the command type
                    command_formatting = config_details.get('command_formatting', {})
                    if command_formatting and command_type:
                        supported_commands = command_formatting.get('validation', {}).get('supported_commands', [])
                        if command_type in supported_commands:
                            selected_translator_config = translator_config
                            break
                    elif not command_type:
                        # If no specific command type, use first matching manufacturer
                        selected_translator_config = translator_config
                        break
                
                # Fallback to first matching manufacturer if no command type match
                if not selected_translator_config and manufacturer:
                    for translator_config in translator_configs:
                        config_details = translator_config.get('config', {})
                        config_manufacturer = config_details.get('manufacturer', '')
                        if config_manufacturer == manufacturer:
                            selected_translator_config = translator_config
                            break
                
                # Final fallback to first available translator
                if not selected_translator_config and translator_configs:
                    selected_translator_config = translator_configs[0]

                if selected_translator_config:
                    translator = TranslatorFactory.create_translator(selected_translator_config)
                    self._translator_cache[cache_key] = translator
                    logger.debug(f"Created translator for manufacturer '{manufacturer}', command_type '{command_type}'")
                else:
                    logger.error(f"No suitable translator configuration found for manufacturer '{manufacturer}', command_type '{command_type}'")
                    return None

            except Exception as e:
                logger.error(f"Failed to create translator: {e}")
                return None

        return self._translator_cache[cache_key]


    
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
        Get a formatted command for a device.
        Commands are formatted by the parser-based system.
        
        Args:
            device_id: The device ID to get a command for
            
        Returns:
            Formatted command bytes or None if no commands available
        """
        if not self.pending_commands.get(device_id):
            return None
            
        # Get the oldest command
        command = self.pending_commands[device_id][0]
        command_id = command.get('request_id', 'unknown')
        
        try:
            # Check if command is formatted by parser
            payload = command.get('payload', {})
            if isinstance(payload, dict) and 'formatted_command' in payload:
                # Command was formatted by parser
                formatted_hex = payload['formatted_command']
                try:
                    formatted_command = bytes.fromhex(formatted_hex)
                    logger.info(f"[{command_id}] Using parser-formatted command for device {device_id}")
                    return formatted_command
                except ValueError as e:
                    logger.error(f"[{command_id}] Invalid hex string in formatted command: {e}")
                    return None
            else:
                # Command is not formatted - this should not happen with the new architecture
                logger.error(f"[{command_id}] Command not formatted for device {device_id}. All commands must be formatted by parser.")
                return None
                
        except Exception as e:
            logger.exception(f"[{command_id}] Error getting formatted command for device {device_id}: {e}")
            return None 