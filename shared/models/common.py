# shared/models/common.py
from typing import Any, Dict, List, Optional, Type
import json
import uuid
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, validator
from pydantic.json import pydantic_encoder

def generate_request_id():
    return str(uuid.uuid4())

class CustomBaseModel(BaseModel):
    """Base model with custom JSON serialization for all models in the application."""
    request_id: str = Field(default_factory=generate_request_id)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda dt: dt.isoformat(),
            bytes: lambda b: b.hex()
        }
    )
    
    def model_dump_json(self, **kwargs) -> str:
        """Serialize model to JSON string with proper handling of datetime and bytes."""
        return json.dumps(self.model_dump(), **kwargs)
    
    def dict(self, **kwargs):
        """Legacy support for Pydantic v1 style dict() method."""
        return self.model_dump(**kwargs)

class BaseMessage(CustomBaseModel):
    request_id: str = Field(default_factory=generate_request_id)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class RawMessage(BaseMessage):
    device_id: str
    payload_hex: str  # Renamed from 'payload' to reflect hex-encoded binary data sent through Kafka
    protocol: str   # e.g., 'mqtt', 'coap'
    metadata: dict = {} # Optional extra metadata from gateway

class ErrorMessage(BaseMessage):
    service: str = "dispatcher"
    error: str
    original_message: dict | None = None # The message that caused the error

class CommandMessage(CustomBaseModel):
    """
    Base model for device commands.
    Represents a command or setting to be pushed to a device.
    """
    device_id: str = Field(..., description="Unique identifier for the target device")
    command_type: str = Field(..., description="Type of command (e.g., 'set_led', 'reboot', 'update_settings')")
    payload: Dict[str, Any] = Field(..., description="Command payload data")
    protocol: str = Field(..., description="Protocol to use (mqtt or coap)")
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Protocol-specific metadata (e.g., MQTT topic/qos/retain, CoAP confirmable/options)"
    )
    priority: Optional[int] = Field(None, description="Command priority level")
    expires_at: Optional[datetime] = Field(None, description="Command expiration time")

    @validator('protocol')
    def validate_protocol(cls, v):
        """Validate the protocol value."""
        valid_protocols = ['mqtt', 'coap']
        if v.lower() not in valid_protocols:
            raise ValueError(f"Protocol must be one of {valid_protocols}")
        return v.lower()

class StandardizedOutput(CustomBaseModel):
    """
    Represents sensor readings with a consistent structure:
    device_id, values, timestamp, metadata, index, optional label.
    """
    device_id: str
    values: List[Any]
    label: Optional[List[str]] = None
    index: str = ""
    metadata: Optional[Dict[str, Any]] = None

class ValidatedOutput(CustomBaseModel):
    """
    Represents validated sensor readings with a consistent structure,
    but without metadata fields. Used for final output after validation.
    """
    device_id: str
    values: List[Any]
    label: Optional[List[str]] = None
    index: str = ""
    metadata: Optional[Dict[str, Any]] = None