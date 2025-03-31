# shared/models/common.py
from typing import Any, Dict, List, Optional
import uuid
from datetime import datetime
from pydantic import BaseModel, Field

def generate_request_id():
    return str(uuid.uuid4())

class BaseMessage(BaseModel):
    request_id: str = Field(default_factory=generate_request_id)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class RawMessage(BaseMessage):
    device_id: str
    payload: bytes # Base64 encoded payload bytes
    protocol: str   # e.g., 'mqtt', 'coap'
    metadata: dict = {} # Optional extra metadata from gateway

class ErrorMessage(BaseMessage):
    service: str = "dispatcher"
    error: str
    original_message: dict | None = None # The message that caused the error

class StandardizedOutput(BaseModel):
    """
    Represents sensor readings with a consistent structure:
    device_id, values, timestamp, metadata, index, optional label.
    """
    device_id: str
    values: List[Any]
    label: Optional[List[str]] = None
    index: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)