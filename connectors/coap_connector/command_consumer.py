# coap_connector/command_consumer.py
import logging
import asyncio
import json
import os
from typing import Dict, Any, Optional, List
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer
from shared.utils.script_client import ScriptClient
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
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
            group_id=f"coap_command_consumer",
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only consume new messages
            on_error_callback=self._on_consumer_error
        )
            
        self.running = True
        self._stop_event.clear()
        logger.info("Command consumer started")
        
    async def consume_commands(self):
        """Start consuming command messages using AsyncResilientKafkaConsumer."""
        if not self.resilient_consumer:
            logger.error("Cannot start consuming: AsyncResilientKafkaConsumer not initialized")
            return
            
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_command_message,
                commit_offset=True,
                batch_processing=False  # Process commands individually
            )
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
            command_data = message.value
            
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

            # Format command using parser-based approach
            formatted_command = await self._format_command_with_parser(device_id, command_message)
            if formatted_command:
                # Store the formatted command
                await self._store_formatted_command(device_id, command_message, formatted_command)
                logger.info(f"[{request_id}] Formatted and stored command for device {device_id}")
                return True

            # If we get here, we couldn't format the command
            logger.error(f"[{request_id}] Unable to format command for device {device_id}: parser script not found or formatting failed")
            return True  # Consider this as processed (won't retry)

        except Exception as e:
            logger.exception(f"Error processing command: {e}")
            return False  # Indicate failure - message will be retried

    async def _store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_command: bytes):
        """Store a command formatted by parser."""
        # Convert formatted binary command to hex string for storage
        hex_command = formatted_command.hex()

        # Update command data with formatted payload
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
        Format command using device-specific parser.
        
        Args:
            device_id: Target device identifier
            command_message: CommandMessage object containing:
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
            metadata = command_message.metadata or {}
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
            command_type = command_message.command_type
            translator = await self._get_translator_for_device(device_id, command_type, manufacturer)

            # Use empty hardware configuration for now
            hardware_config = {}

            # Format command using parser - convert CommandMessage to dict for parser
            command_dict = command_message.model_dump()
            binary_command = script_module.format_command(
                command=command_dict,
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