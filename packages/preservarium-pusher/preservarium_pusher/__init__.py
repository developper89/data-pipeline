# preservarium_pusher/__init__.py
"""
Preservarium Pusher module for sending commands to IoT devices.

This module provides a way to push commands and settings to devices through
various protocols like MQTT and CoAP, using the existing connector infrastructure.
"""

__version__ = "0.1.0"

from typing import Optional, Dict, Any
import asyncio

from preservarium_pusher.service import PusherService
from preservarium_pusher.core.models import CommandMessage, DeviceCommand, DeviceSettings

async def create_pusher_service(
    kafka_bootstrap_servers: str,
    command_topic: str,
) -> Optional[PusherService]:
    """
    Factory function to create and initialize a PusherService.
    
    Args:
        kafka_bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        command_topic: The Kafka topic for publishing device commands
        
    Returns:
        An initialized PusherService or None if initialization failed
    """
    service = PusherService(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        command_topic=command_topic,
    )
    
    # Initialize the service
    if await service.initialize():
        return service
    return None

# Synchronous version of the factory function for non-async environments
def create_pusher_service_sync(
    kafka_bootstrap_servers: str,
    command_topic: str,
) -> Optional[PusherService]:
    """
    Synchronous factory function to create and initialize a PusherService.
    
    Args:
        kafka_bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        command_topic: The Kafka topic for publishing device commands
        
    Returns:
        An initialized PusherService or None if initialization failed
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            create_pusher_service(
                kafka_bootstrap_servers=kafka_bootstrap_servers,
                command_topic=command_topic,
            )
        )
    finally:
        loop.close()
        
# Export key classes for direct import
__all__ = [
    'CommandMessage', 
    'DeviceCommand', 
    'DeviceSettings',
    'PusherService',
    'create_pusher_service',
    'create_pusher_service_sync',
] 