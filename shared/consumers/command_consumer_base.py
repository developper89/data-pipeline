# shared/consumers/command_consumer_base.py
import logging
import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

from confluent_kafka import KafkaError, KafkaException

from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer
from shared.config_loader import get_translator_configs
from shared.translation.factory import TranslatorFactory
from shared.models.common import CommandMessage

logger = logging.getLogger(__name__)

class BaseCommandConsumer(ABC):
    """
    Base class for command consumers that handles common functionality.
    Concrete implementations provide protocol-specific behavior.
    """
    
    def __init__(self, consumer_group_id: str, protocol: str, kafka_topic: str, kafka_bootstrap_servers: str):
        """
        Initialize the base command consumer.
        
        Args:
            consumer_group_id: Kafka consumer group ID
            protocol: Protocol name (e.g., 'coap', 'mqtt')
            kafka_topic: Kafka topic to consume from
            kafka_bootstrap_servers: Kafka bootstrap servers
        """
        self.consumer_group_id = consumer_group_id
        self.protocol = protocol.lower()
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        self.running = False
        self._stop_event = asyncio.Event()
        self._consumer_task = None
        
        # In-memory store for pending commands, indexed by device_id
        # In a production system, this would be a persistent store
        self.pending_commands: Dict[str, List[Dict[str, Any]]] = {}
        
        # Cache for loaded translators
        self._translator_cache = {}
        logger.debug(f"{self.protocol.upper()} command consumer initialized with translator-based formatting support")
        
    async def start(self):
        """Start the command consumer."""
        if self.running:
            logger.warning(f"{self.protocol.upper()} command consumer already running")
            return
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=self.kafka_topic,
            group_id=self.consumer_group_id,
            bootstrap_servers=self.kafka_bootstrap_servers,
            auto_offset_reset='latest',  # Only consume new messages
            on_error_callback=self._on_consumer_error
        )
            
        self.running = True
        self._stop_event.clear()
        
        # Start consuming in a separate asyncio task for MQTT, direct for CoAP
        if hasattr(self, 'mqtt_client'):  # MQTT consumer
            self._consumer_task = asyncio.create_task(self._consume_commands())
            logger.info(f"Async {self.protocol.upper()} command consumer started")
        else:  # CoAP consumer
            logger.debug(f"{self.protocol.upper()} command consumer started")
        
    async def consume_commands(self):
        """Start consuming command messages using AsyncResilientKafkaConsumer."""
        logger.debug(f"Starting {self.protocol.upper()} command consumption")
        
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
            logger.info(f"{self.protocol.upper()} command consumer completed")
        except Exception as e:
            logger.exception(f"Fatal error in {self.protocol.upper()} command consumer: {e}")
            self.running = False

    async def _consume_commands(self):
        """Internal consume method for MQTT (async task wrapper)."""
        if not self.resilient_consumer:
            logger.error("Cannot start consuming: AsyncResilientKafkaConsumer not initialized")
            return
            
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_command_message,
                commit_offset=True
            )
        except Exception as e:
            logger.exception(f"Fatal error in {self.protocol.upper()} command consumer: {e}")
            self.running = False

    async def stop(self):
        """Stop the command consumer."""
        if not self.running:
            logger.warning(f"{self.protocol.upper()} command consumer not running")
            return
            
        logger.info(f"Stopping {self.protocol.upper()} command consumer...")
        self.running = False
        self._stop_event.set()
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
            
        # Wait for consumer task to finish (MQTT only)
        if self._consumer_task and not self._consumer_task.done():
            try:
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"{self.protocol.upper()} command consumer task did not finish within timeout")
                self._consumer_task.cancel()
                try:
                    await self._consumer_task
                except asyncio.CancelledError:
                    pass
            
        logger.info(f"{self.protocol.upper()} command consumer stopped")

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
        logger.error(f"{self.protocol.upper()} consumer error: {error}")
        # Could implement additional error handling logic here if needed

    async def _process_command(self, command_message: CommandMessage) -> bool:
        """
        Process command using parser-based formatting.

        Args:
            command_message: The CommandMessage from Kafka
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Check if this consumer should process this command
            if not self.should_process_command(command_message):
                logger.debug(f"Ignoring non-{self.protocol.upper()} command: {command_message.protocol}")
                return True

            # Extract required fields
            device_id = command_message.device_id
            request_id = command_message.request_id

            if not device_id:
                logger.error(f"[{request_id}] Invalid command message: missing device_id")
                return True

            # Handle feedback acknowledgment for MQTT
            if hasattr(self, 'acknowledge_command') and command_message.command_type == "feedback":
                logger.debug(f"[{command_message.request_id}] Received feedback acknowledgment for device {command_message.device_id}")
                pending_commands = self.pending_commands.get(command_message.device_id, None)
                
                if pending_commands:
                    success = self.acknowledge_command(command_message.device_id, command_message.request_id)
                    if success:
                        logger.info(f"[{command_message.request_id}] Command {command_message.request_id} acknowledged")
                    else:
                        logger.warning(f"[{command_message.request_id}] Failed to acknowledge command {command_message.request_id}")
                return True  # Successfully processed feedback

            # Format command and response using parser-based approach
            formatted_data = await self.format_command_with_parser(device_id, command_message)
            if formatted_data:
                # Store the formatted command and response
                await self.store_formatted_command(device_id, command_message, formatted_data)
                
                # Handle protocol-specific processing
                success = await self.handle_protocol_specific_processing(command_message, formatted_data)
                if success:
                    logger.info(f"[{request_id}] Formatted and processed command for device {device_id}")
                    return True
                else:
                    logger.error(f"[{request_id}] Failed protocol-specific processing for device {device_id}")
                    return False

            # If we get here, we couldn't format the command
            logger.error(f"[{request_id}] Unable to format command for device {device_id}: parser script not found or formatting failed")
            return True  # Consider this as processed (won't retry)

        except Exception as e:
            logger.exception(f"Error processing command: {e}")
            return False  # Indicate failure - message will be retried

    async def store_formatted_command(self, device_id: str, command_message: CommandMessage, formatted_data: Any):
        """
        Store formatted command with protocol-agnostic handling.
        
        Args:
            device_id: Target device identifier
            command_message: Original CommandMessage
            formatted_data: Formatted command data (bytes for CoAP, dict for MQTT, etc.)
        """
        # Handle protocol-specific formatting for storage
        if isinstance(formatted_data, bytes):
            # CoAP-style: convert bytes to hex string
            storage_data = formatted_data.hex() if formatted_data else ""
        else:
            # MQTT-style: store as-is (dict, string, etc.)
            storage_data = formatted_data

        # Update command data with formatted payload
        enhanced_command = command_message.model_dump()
        enhanced_command['payload'] = {
            'formatted_command': storage_data,
            'formatted_by': 'parser',
            'original_payload': command_message.payload
        }
        enhanced_command['stored_at'] = time.time()

        # Store the enhanced command
        if device_id not in self.pending_commands:
            self.pending_commands[device_id] = []
        self.pending_commands[device_id].append(enhanced_command)

    async def format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Any:
        """
        Format command using protocol-specific parser from translator.
        
        Args:
            device_id: Target device identifier
            command_message: CommandMessage object containing:
                - device_id: Target device identifier
                - command_type: Type of command (e.g., "alarm", "config")
                - payload: Command-specific data
                - protocol: Communication protocol
                - metadata: Optional metadata containing manufacturer information
            
        Returns:
            Formatted command data (bytes for CoAP, dict for MQTT, etc.) or None if formatting fails
        """
        try:
            # Get metadata from command message
            metadata = command_message.metadata
            manufacturer = metadata.get('manufacturer')
            command_type = command_message.command_type
            protocol = command_message.protocol
            
            # Get translator instance based on command type and manufacturer
            translator = await self._get_translator_for_manufacturer(command_type, protocol, manufacturer)
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
            
            # Dynamic parameter handling based on translator capabilities
            formatted_command = script_module.format_command(
                command=command_dict,
                message_parser=translator.message_parser if hasattr(translator, 'message_parser') else None,
                config={}
            )

            return formatted_command

        except Exception as e:
            logger.exception(f"Error formatting command for device {device_id}: {e}")
            return None

    async def _get_translator_for_manufacturer(self, command_type: str = None, protocol: str = None, manufacturer: str = None):
        """Get translator instance for device based on command type and manufacturer."""
        cache_key = f"{manufacturer}_translator"
        if cache_key in self._translator_cache:
            logger.debug(f"Translator for {cache_key} found in cache")
            return self._translator_cache[cache_key]

        try:
            # Get translator configs for this connector
            translator_configs = get_translator_configs(f"{protocol}-connector")
            # Find appropriate translator based on manufacturer and command type
            selected_translator_config = None
            
            for translator_config in translator_configs:
                config_details = translator_config.get('config', {})
                
                # Check manufacturer match first (most important)
                config_manufacturer = config_details.get('manufacturer', '')
                if manufacturer and config_manufacturer != manufacturer:
                    continue
                
                # Check if this translator supports the command type
                commands = config_details.get('commands', {})
                if commands and command_type:
                    supported_commands = commands.get('validation', {}).get('supported_commands', [])
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
                return translator
            else:
                logger.error(f"No suitable translator configuration found for manufacturer '{manufacturer}', command_type '{command_type}'")
                return None

        except Exception as e:
            logger.error(f"Failed to create translator: {e}")
            return None

    # Abstract methods that must be implemented by concrete classes
    
    @abstractmethod
    def should_process_command(self, command_message: CommandMessage) -> bool:
        """
        Check if this consumer should process the given command.
        
        Args:
            command_message: The CommandMessage from Kafka
            
        Returns:
            True if this consumer should process the command, False otherwise
        """
        pass

    @abstractmethod
    async def handle_protocol_specific_processing(self, command_message: CommandMessage, formatted_data: Any) -> bool:
        """
        Handle protocol-specific command processing (e.g., MQTT publishing, CoAP storage).
        
        Args:
            command_message: Original CommandMessage
            formatted_data: Formatted command data
            
        Returns:
            True if processing was successful, False otherwise
        """
        pass 