# coap_connector/command_consumer.py
import logging
import asyncio
import json
import os
from typing import Dict, Any, Optional, List
import time

# Remove kafka-python imports - using confluent-kafka through shared helpers
from confluent_kafka import KafkaError, KafkaException

from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer
from shared.config_loader import get_translator_configs
from shared.translation.factory import TranslatorFactory
from shared.models.common import CommandMessage
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
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        self.running = False
        self._stop_event = asyncio.Event()
        
        # In-memory store for pending commands, indexed by device_id
        # In a production system, this would be a persistent store
        self.pending_commands: Dict[str, List[Dict[str, Any]]] = {}
        
        # Cache for loaded translators
        self._translator_cache = {}
        logger.debug("Command consumer initialized with translator-based formatting support")
        
    async def start(self):
        """Start the command consumer."""
        if self.running:
            logger.warning("Command consumer already running")
            return
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
            group_id=f"iot_command_consumer_coap",
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only consume new messages
            on_error_callback=self._on_consumer_error
        )
            
        self.running = True
        self._stop_event.clear()
        logger.debug("Command consumer started")
        
    async def consume_commands(self):
        """Start consuming command messages using AsyncResilientKafkaConsumer."""
        logger.debug("Starting command consumption")
        
        if not self.resilient_consumer:
            logger.error("Cannot start consuming: AsyncResilientKafkaConsumer not initialized")
            return
            
        logger.debug(f"Starting consumption for topic '{self.resilient_consumer.topic}' with group '{self.resilient_consumer.group_id}'")
        
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_command_message,
                commit_offset=True,
                batch_processing=False  # Process commands individually
            )
            logger.info("Command consumer completed")
        except Exception as e:
            logger.exception(f"Fatal error in command consumer: {e}")
            self.running = False

    async def _process_command_message(self, message) -> bool:
        """
        Process a single command message.
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Message value is already deserialized by KafkaConsumer
            command_data = message.parsed_value
            
            # Parse message into CommandMessage model
            try:
                if isinstance(command_data, (bytes, str)):
                    # If message is bytes/string, decode as JSON
                    message_data = json.loads(command_data) if isinstance(command_data, str) else json.loads(command_data.decode('utf-8'))
                else:
                    # If message is already a dict
                    message_data = command_data

                command_message = CommandMessage(**message_data)
                success = await self._process_command(command_message)
                return success
            except Exception as e:
                logger.error(f"Failed to parse command message: {e}")
                return True  # Consider invalid message as processed (won't retry)
                
        except Exception as e:
            logger.exception(f"Unexpected error processing command message: {e}")
            return False  # Indicate failure - message will be retried
            
    async def _on_consumer_error(self, error, context):
        """Handle consumer-level errors."""
        logger.error(f"Consumer error: {error}")
        # Could implement additional error handling logic here if needed

    async def stop(self):
        """Stop the command consumer."""
        if not self.running:
            logger.warning("Command consumer not running")
            return
            
        logger.info("Stopping command consumer...")
        self.running = False
        self._stop_event.set()
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
            
        logger.info("Command consumer stopped")
    
    async def _process_command(self, command_message: CommandMessage):
        """
        Process command using parser-based formatting.

        Args:
            command_message: The CommandMessage from Kafka
        """
        try:
            # Check if this is a CoAP command
            if command_message.protocol.lower() != 'coap':
                logger.debug(f"Ignoring non-CoAP command: {command_message.protocol}")
                return True

            # Extract required fields
            device_id = command_message.device_id
            request_id = command_message.request_id

            if not device_id:
                logger.error(f"[{request_id}] Invalid command message: missing device_id")
                return True

            # Format command and response using parser-based approach
            formatted_data = await self._format_command_with_parser(device_id, command_message)
            if formatted_data:
                # Store the formatted command and response
                await self._store_formatted_command(device_id, command_message, formatted_data)
                logger.info(f"[{request_id}] Formatted and stored command with response for device {device_id}")
                return True

            # If we get here, we couldn't format the command
            logger.error(f"[{request_id}] Unable to format command and response for device {device_id}: parser script not found or formatting failed")
            return True  # Consider this as processed (won't retry)

        except Exception as e:
            logger.exception(f"Error processing command: {e}")
            return False  # Indicate failure - message will be retried

    async def _store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_data: bytes):
        """Store a command formatted by parser."""
        command_bytes = formatted_data
        
        # Convert formatted binary command and response to hex strings for storage
        hex_command = command_bytes.hex() if command_bytes else ""

        # Update command data with formatted payload and response
        enhanced_command = command_message.model_dump()
        enhanced_command['payload'] = {
            'formatted_command': hex_command,
            'formatted_by': 'parser',
            'original_payload': command_message.payload
        }
        enhanced_command['stored_at'] = time.time()

        # Store the enhanced command
        if device_id not in self.pending_commands:
            self.pending_commands[device_id] = []
        self.pending_commands[device_id].append(enhanced_command)

    async def _format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Optional[bytes]:
        """
        Format command and response using device-specific parser from translator.
        
        Args:
            device_id: Target device identifier
            command_message: CommandMessage object containing:
                - device_id: Target device identifier
                - command_type: Type of command (e.g., "alarm", "config")
                - payload: Command-specific data
                - protocol: Communication protocol
                - metadata: Optional metadata containing manufacturer information
            
        Returns:
            bytes: command_bytes or None if formatting fails
        """
        try:
            # Get metadata from command message
            metadata = command_message.metadata
            manufacturer = metadata.get('manufacturer')
            command_type = command_message.command_type
            logger.debug(f"Formatting command for device {device_id} {command_message.model_dump()}")
            # Get translator instance based on command type and manufacturer
            translator = await self._get_translator_for_manufacturer(command_type, manufacturer)
            if not translator:
                logger.error(f"No translator found for device {device_id} with manufacturer {manufacturer}")
                return None

            # Get script module from translator
            script_module = None
            if hasattr(translator, 'script_module') and translator.script_module:
                script_module = translator.script_module
            elif hasattr(translator, 'parser_script_path') and translator.parser_script_path:
                # Try to load the script module if it's not already loaded
                try:
                    await translator.load_script_module_async()
                    script_module = translator.script_module
                except Exception as e:
                    logger.error(f"Failed to load script module for translator: {e}")
                    return None

            if not script_module:
                logger.error(f"No script module available for device {device_id}")
                return None

            # Check if parser supports command formatting
            if not hasattr(script_module, 'format_command'):
                logger.error(f"Parser for device {device_id} does not support command formatting")
                return None

            # Format command using parser - convert CommandMessage to dict for parser
            command_dict = command_message.model_dump()
            
            # Format command
            binary_command = script_module.format_command(
                command=command_dict,
                message_parser=translator.message_parser,
                config={}
            )

            return binary_command

        except Exception as e:
            logger.exception(f"Error formatting command and response for device {device_id}: {e}")
            return None

# Removed _get_parser_module method - now using script module directly from translator

    async def _get_translator_for_manufacturer(self, command_type: str = None, manufacturer: str = None):
        """Get translator instance for device based on command type and manufacturer."""
        cache_key = f"{manufacturer}_translator"
        if cache_key in self._translator_cache:
            logger.debug(f"Translator for {cache_key} found in cache")
            return self._translator_cache[cache_key]

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
                
                # Load script module if translator has parser_script_path
                if hasattr(translator, 'parser_script_path') and translator.parser_script_path:
                    try:
                        await translator.load_script_module_async()
                        logger.debug(f"Loaded script module for translator '{manufacturer}', command_type '{command_type}'")
                    except Exception as e:
                        logger.warning(f"Failed to load script module for translator: {e}")
                
                self._translator_cache[cache_key] = translator
                logger.debug(f"Created translator for manufacturer '{manufacturer}', command_type '{command_type}'")
            else:
                logger.error(f"No suitable translator configuration found for manufacturer '{manufacturer}', command_type '{command_type}'")
                return None

        except Exception as e:
            logger.error(f"Failed to create translator: {e}")
            return None


    
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
