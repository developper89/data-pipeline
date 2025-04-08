# shared/models/common.py
from typing import Any, Dict, List, Optional, Type
import json
import uuid
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
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
    payload: bytes # Payload bytes will be hex-encoded in JSON
    protocol: str   # e.g., 'mqtt', 'coap'
    metadata: dict = {} # Optional extra metadata from gateway

class ErrorMessage(BaseMessage):
    service: str = "dispatcher"
    error: str
    original_message: dict | None = None # The message that caused the error

class StandardizedOutput(CustomBaseModel):
    """
    Represents sensor readings with a consistent structure:
    device_id, values, timestamp, metadata, index, optional label.
    """
    device_id: str
    values: List[Any]
    label: Optional[List[str]] = None
    index: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)