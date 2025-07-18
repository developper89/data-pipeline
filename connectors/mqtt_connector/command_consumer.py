# mqtt_connector/command_consumer.py
import logging
import json
import asyncio
from typing import Dict, Any, Optional

from shared.config_loader import get_translator_configs
from shared.models.common import CommandMessage
from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer
from client import MQTTClientWrapper
import config
from shared.translation.factory import TranslatorFactory

logger = logging.getLogger(__name__)

class CommandConsumer:
    """
    Consumes device command messages from Kafka and publishes them to MQTT topics.
    Uses AsyncResilientKafkaConsumer for automatic error handling and reconnection.
    """
    
    def __init__(self, mqtt_client: MQTTClientWrapper):
        """
        Initialize the command consumer.
        
        Args:
            mqtt_client: The MQTT client wrapper to use for publishing
        """
        self.mqtt_client = mqtt_client
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        self.running = False
        self._consumer_task = None
        self._translator_cache = {}

    async def start(self):
        """Start the command consumer with AsyncResilientKafkaConsumer."""
        if self.running:
            logger.warning("Command consumer already running")
            return
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
            group_id="iot_command_consumer_mqtt",
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only consume new messages
            on_error_callback=self._on_consumer_error
        )
            
        self.running = True
        
        # Start consuming in a separate asyncio task
        self._consumer_task = asyncio.create_task(self._consume_commands())
        
        logger.info("Async command consumer started")

    async def stop(self):
        """Stop the command consumer."""
        if not self.running:
            logger.warning("Command consumer not running")
            return
            
        self.running = False
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
            
        # Wait for consumer task to finish
        if self._consumer_task and not self._consumer_task.done():
            try:
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Command consumer task did not finish within timeout")
                self._consumer_task.cancel()
                try:
                    await self._consumer_task
                except asyncio.CancelledError:
                    pass
            
        logger.info("Async command consumer stopped")

    async def _consume_commands(self):
        """Start consuming command messages using AsyncResilientKafkaConsumer."""
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
            logger.exception(f"Fatal error in command consumer: {e}")
            self.running = False

    async def _format_command_with_parser(self, device_id: str, command_message: CommandMessage) -> Optional[dict]:
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
            dict: command_dict or None if formatting fails
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
            
            # Format command
            command_dict = script_module.format_command(
                command=command_message.model_dump(),
                config={}
            )

            return command_dict

        except Exception as e:
            logger.exception(f"Error formatting command and response for device {device_id}: {e}")
            return None
        
    async def _process_command_message(self, message):
        """
        Process a single command message.
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Process the command
            command_message = CommandMessage(**message.parsed_value)
            success = await self._process_command(command_message)
            return success
            
        except Exception as e:
            logger.exception(f"Unexpected error processing command message: {e}")
            return False  # Don't commit offset on error
    
    async def _on_consumer_error(self, error, context):
        """Handle consumer-level errors."""
        logger.error(f"Consumer error: {error}")
        # Could implement additional error handling logic here if needed
    
    async def _process_command(self, command_data: CommandMessage) -> bool:
        """
        Process a command message and publish it to MQTT.
        
        Args:
            command_data: The command data from Kafka
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Check if this is an MQTT command
            if command_data.protocol.lower() != 'mqtt':
                logger.debug(f"Ignoring non-MQTT command: {command_data.protocol}")
                return True  # Successfully ignored
                
            # Extract required fields
            device_id = command_data.device_id
            payload = command_data.payload
            request_id = command_data.request_id
            formatted_data = await self._format_command_with_parser(device_id, command_data)
            if not formatted_data:
                logger.error(f"[{request_id}] Failed to format command for device {device_id}")
                return False
            
            metadata = formatted_data.get('metadata', {})
            # Extract topic from metadata (required for MQTT)
            topic = metadata.get('topic', None)
            if not topic:
                logger.error(f"[{request_id}] MQTT command missing topic in metadata for device {device_id}")
                return False
            
            # Extract optional MQTT-specific fields from metadata
            qos = metadata.get('qos', None)
            retain = metadata.get('retain', None)
            
            if not device_id or payload is None:
                logger.error(f"[{request_id}] Invalid command message: missing required fields")
                return False
                
            logger.info(f"[{request_id}] Processing command for device {device_id} on topic {topic}")
            
            # Publish the message to MQTT using executor for the synchronous call
            success = await asyncio.get_event_loop().run_in_executor(
                None,
                self.mqtt_client.publish_message,
                topic,
                formatted_data.get('payload', {}),
                qos,
                retain
            )
            
            if success:
                logger.info(f"[{request_id}] Successfully published command to {topic}")
                return True
            else:
                logger.error(f"[{request_id}] Failed to publish command to {topic}")
                return False
                
        except Exception as e:
            logger.exception(f"Error processing command: {e}")
            return False 
        
    async def _get_translator_for_manufacturer(self, command_type: str = None, manufacturer: str = None):
        """Get translator instance for device based on command type and manufacturer."""
        cache_key = f"{manufacturer}_translator"
        if cache_key in self._translator_cache:
            logger.debug(f"Translator for {cache_key} found in cache")
            return self._translator_cache[cache_key]

        try:
            # Get translator configs for mqtt-connector
            translator_configs = get_translator_configs("mqtt-connector")

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
