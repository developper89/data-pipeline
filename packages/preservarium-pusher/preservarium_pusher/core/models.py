from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import Field, validator

from shared.models.common import CustomBaseModel, generate_request_id

class CommandMessage(CustomBaseModel):
    """
    Base model for device commands.
    Represents a command or setting to be pushed to a device.
    """
    device_id: str
    command_type: str  # e.g., "set_led", "reboot", "update_settings"
    payload: Dict[str, Any]  # Command data
    protocol: str  # e.g., "mqtt", "coap"
    parser_script_ref: Optional[str] = None  # Parser script reference (e.g., "efento_bidirectional_parser.py")
    metadata: Optional[Dict[str, Any]] = None  # Protocol-specific metadata (e.g., MQTT topic, CoAP options)
    qos: Optional[int] = None  # Quality of Service for MQTT
    retain: Optional[bool] = None  # Retain flag for MQTT
    confirmable: Optional[bool] = None  # Confirmable flag for CoAP
    
    @validator('protocol')
    def validate_protocol(cls, v):
        """Validate the protocol value."""
        valid_protocols = ['mqtt', 'coap']
        if v.lower() not in valid_protocols:
            raise ValueError(f"Protocol must be one of {valid_protocols}")
        return v.lower()
    
    @validator('parser_script_ref')
    def validate_parser_script_ref(cls, v):
        """Validate the parser script reference if provided."""
        if v and not v.endswith('.py'):
            raise ValueError("Parser script reference should end with .py extension")
        return v

class DeviceCommand(CommandMessage):
    """
    Specific model for device commands (e.g., set LED state).
    Represents a one-time command to change a device's state.
    """
    # Default command_type for device commands
    command_type: str = "command"
    
    # Additional metadata specific to device commands
    priority: Optional[int] = None  # Priority level for the command
    expires_at: Optional[datetime] = None  # Command expiration time

class DeviceSettings(CommandMessage):
    """
    Model for persistent device settings.
    Represents a configuration that should be persisted on the device.
    """
    # Default command_type for settings updates
    command_type: str = "settings"
    
    # Additional metadata specific to device settings
    persistent: bool = True  # Flag indicating this is a persistent setting
    version: Optional[int] = None  # Version of the settings configuration 