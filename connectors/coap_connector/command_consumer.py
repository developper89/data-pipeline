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
from shared.consumers.command_consumer_base import BaseCommandConsumer

logger = logging.getLogger(__name__)

class CommandConsumer(BaseCommandConsumer):
    """
    CoAP command consumer that uses parser-based command formatting.
    All commands are formatted using device-specific parsers and stored for retrieval.
    """
    
    def __init__(self):
        """
        Initialize the CoAP command consumer.
        """
        super().__init__(
            consumer_group_id="iot_command_consumer_coap",
            protocol="coap",
            kafka_topic=config.KAFKA_DEVICE_COMMANDS_TOPIC,
            kafka_bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
        
        # In-memory store for pending commands, indexed by device_id
        # In a production system, this would be a persistent store
        self.pending_commands: Dict[str, List[Dict[str, Any]]] = {}
        
        # Cache for loaded translators
        self._translator_cache = {}
        logger.debug("Command consumer initialized with translator-based formatting support")
        
    def should_process_command(self, command_message: CommandMessage) -> bool:
        """Check if this is a CoAP command."""
        return command_message.protocol.lower() == 'coap'

    async def handle_protocol_specific_processing(self, command_message: CommandMessage, formatted_data: bytes) -> bool:
        """
        CoAP-specific processing: just store the command (no additional processing needed).
        
        Args:
            command_message: Original CommandMessage
            formatted_data: Formatted command bytes
            
        Returns:
            True (always successful for CoAP storage)
        """
        # CoAP commands are just stored for retrieval, no additional processing
        return True
