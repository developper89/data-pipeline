from typing import Optional, Tuple, Any, Dict
import logging
from .proto_compiler import ProtobufCompiler, check_protoc_available

logger = logging.getLogger(__name__)

class ProtobufMessageParser:
    """Generic protobuf message parser that works with any manufacturer's schemas."""

    def __init__(self, manufacturer: str, message_types: Dict[str, Any]):
        self.manufacturer = manufacturer
        self.message_types = message_types
        self.compiler = None
        self._load_proto_modules()

    def _load_proto_modules(self):
        """Load compiled protobuf modules for this manufacturer."""
        self.proto_modules = {}
        
        # Check if protobuf is available
        try:
            import google.protobuf
        except ImportError:
            logger.error("protobuf package not installed. Run: pip install protobuf>=4.21.0")
            return

        # Initialize dynamic compiler
        self.compiler = ProtobufCompiler(self.manufacturer)
        
        # First, try to load existing compiled modules
        existing_modules_loaded = self._try_load_existing_modules()
        
        if not existing_modules_loaded:
            # If existing modules couldn't be loaded, compile them
            logger.info(f"Existing compiled modules not found for {self.manufacturer}, compiling...")
            
            # Check if protoc compiler is available
            if not check_protoc_available():
                logger.error("protoc compiler not available. Please install protobuf compiler.")
                return

            # Compile .proto files to Python modules
            compiled_modules = self.compiler.compile_proto_files()
            
            if not compiled_modules:
                logger.warning(f"No protobuf modules compiled for {self.manufacturer}")
                return
            
            logger.info(f"Successfully compiled protobuf modules for {self.manufacturer}")

        # Map message types to compiled classes
        for message_type, config in self.message_types.items():
            try:
                proto_module = config.get('proto_module')
                proto_class = config.get('proto_class')

                if proto_module and proto_class:
                    cls = self.compiler.get_proto_class(proto_module, proto_class)
                    
                    if cls:
                        self.proto_modules[message_type] = {
                            'class': cls,
                            'required_fields': config.get('required_fields', [])
                        }
                        logger.debug(f"Loaded {message_type} class: {proto_class}")
                    else:
                        logger.warning(f"Failed to get class {proto_class} from module {proto_module}")
                        
            except Exception as e:
                logger.warning(f"Failed to load {message_type} for {self.manufacturer}: {e}")
                continue

        logger.debug(f"Successfully loaded {len(self.proto_modules)} protobuf message types for {self.manufacturer}")

    def _try_load_existing_modules(self) -> bool:
        """
        Try to load existing compiled protobuf modules.
        
        Returns:
            bool: True if existing modules were successfully loaded, False otherwise
        """
        try:
            # Use the compiler's method to try loading existing modules
            return self.compiler.try_load_existing_modules()
            
        except Exception as e:
            logger.debug(f"Failed to load existing modules for {self.manufacturer}: {e}")
            return False

    def detect_message_type(self, payload: bytes) -> Optional[str]:
        """Detect the type of protobuf message."""
        logger.debug(f"Detecting message type for payload of {len(payload)} bytes")
        
        for message_type, proto_info in self.proto_modules.items():
            logger.debug(f"Trying to parse as {message_type}...")
            if self._try_parse_message(payload, proto_info, message_type):
                logger.debug(f"Successfully detected message type: {message_type}")
                return message_type
            else:
                logger.debug(f"Failed to parse as {message_type}")
                
        logger.warning(f"Could not detect message type for payload. Available types: {list(self.proto_modules.keys())}")
        return None

    def parse_message(self, payload: bytes) -> Tuple[str, Any]:
        """Parse protobuf message and return type and parsed object."""
        logger.debug(f"Parsing protobuf message of {len(payload)} bytes")
        
        message_type = self.detect_message_type(payload)

        if message_type and message_type in self.proto_modules:
            proto_info = self.proto_modules[message_type]
            message = proto_info['class']()
            message.ParseFromString(payload)
            logger.debug(f"Successfully parsed {message_type} message")
            
            # Log available fields for debugging
            if hasattr(message, 'DESCRIPTOR'):
                available_fields = [field.name for field in message.DESCRIPTOR.fields]
                # logger.debug(f"Parsed {message_type} has fields: {available_fields}")
            
            return message_type, message
        else:
            available_types = list(self.proto_modules.keys())
            logger.error(f"Unknown {self.manufacturer} message type. Available types: {available_types}")
            raise ValueError(f"Unknown {self.manufacturer} message type")
    
    def parse_message_as_type(self, payload: bytes, target_message_type: str) -> Tuple[Optional[str], Any]:
        """
        Try to parse protobuf message as a specific message type.
        
        Args:
            payload: Raw protobuf bytes
            target_message_type: Specific message type to try parsing as
            
        Returns:
            Tuple of (message_type, parsed_message) if successful, (None, None) if failed
        """
        logger.debug(f"Trying to parse payload as specific message type: {target_message_type}")
        
        if target_message_type not in self.proto_modules:
            available_types = list(self.proto_modules.keys())
            logger.warning(f"Target message type '{target_message_type}' not available. Available types: {available_types}")
            return None, None
        
        proto_info = self.proto_modules[target_message_type]
        
        if self._try_parse_message(payload, proto_info, target_message_type):
            # Successfully parsed - create and return the parsed message
            message = proto_info['class']()
            message.ParseFromString(payload)
            logger.debug(f"Successfully parsed as {target_message_type} message")
            
            # Log available fields for debugging
            if hasattr(message, 'DESCRIPTOR'):
                available_fields = [field.name for field in message.DESCRIPTOR.fields]
                logger.debug(f"Parsed {target_message_type} has fields: {available_fields}")
            
            return target_message_type, message
        else:
            logger.debug(f"Failed to parse payload as {target_message_type}")
            return None, None

    def parse_message_to_dict(self, payload: bytes) -> Tuple[Optional[str], Optional[Dict]]:
        """
        Parse protobuf message and return type and dictionary representation.
        
        Args:
            payload: Raw protobuf bytes
            
        Returns:
            Tuple of (message_type, message_dict) if successful, (None, None) if failed
        """
        try:
            message_type, parsed_message = self.parse_message(payload)
            
            if not message_type or not parsed_message:
                return None, None
            
            # Convert protobuf message to dictionary
            try:
                from google.protobuf.json_format import MessageToDict
                message_dict = MessageToDict(parsed_message, preserving_proto_field_name=True)
                logger.debug(f"Successfully converted {message_type} to dictionary")
                return message_type, message_dict
            except ImportError:
                logger.error("protobuf package not available for MessageToDict conversion")
                return None, None
            except Exception as e:
                logger.error(f"Failed to convert {message_type} to dictionary: {e}")
                return None, None
                
        except Exception as e:
            logger.error(f"Failed to parse message to dict: {e}")
            return None, None

    def _try_parse_message(self, payload: bytes, proto_info: Dict, message_type: str = "") -> bool:
        """Try to parse payload as specific message type."""
        try:
            message = proto_info['class']()
            message.ParseFromString(payload)

            # Validate required fields exist
            required_fields = proto_info.get('required_fields', [])
            if required_fields:
                logger.debug(f"Checking required fields for {message_type}: {required_fields}")
                
            for field in required_fields:
                if not hasattr(message, field):
                    logger.debug(f"Required field '{field}' not found in {message_type}")
                    return False
                
                # Check if field has a value (protobuf fields can exist but be empty)
                field_value = getattr(message, field)
                if field_value is None or (isinstance(field_value, (str, bytes)) and not field_value):
                    logger.debug(f"Required field '{field}' is empty in {message_type}")
                    return False
                    
                logger.debug(f"Required field '{field}' found with value in {message_type}")

            logger.debug(f"Successfully parsed and validated {message_type}")
            return True
            
        except Exception as e:
            logger.debug(f"Failed to parse as {message_type}: {e}")
            return False
    
    def get_field_types(self, message_type: str) -> Dict[str, str]:
        """
        Get field type information for a specific message type.
        
        Args:
            message_type: The message type to get field types for (e.g., 'config', 'measurements')
            
        Returns:
            Dict mapping field names to their data type categories
            ('integer', 'string', 'boolean', 'bytes', 'array', 'object', 'enum')
        """
        field_types = {}
        
        try:
            if message_type not in self.proto_modules:
                logger.warning(f"Message type '{message_type}' not found in loaded proto modules. Available types: {list(self.proto_modules.keys())}")
                return field_types
            
            proto_info = self.proto_modules[message_type]
            if 'class' not in proto_info:
                logger.warning(f"No protobuf class found for message type '{message_type}'")
                return field_types
                
            proto_class = proto_info['class']
            temp_instance = proto_class()
            
            if hasattr(temp_instance, 'DESCRIPTOR'):
                logger.debug(f"Extracting field types for {message_type} from protobuf schema")
                for field_desc in temp_instance.DESCRIPTOR.fields:
                    field_types[field_desc.name] = self._get_protobuf_field_type_category(field_desc)
                logger.debug(f"Extracted field types for {len(field_types)} fields from {message_type} schema")
            else:
                logger.warning(f"Protobuf class for {message_type} has no DESCRIPTOR attribute")
                
        except Exception as e:
            logger.error(f"Error extracting field types for {message_type}: {e}")
            
        return field_types
    
    def get_field_type(self, message_type: str, field_name: str) -> Optional[str]:
        """
        Get the data type category for a specific field in a message type.
        
        Args:
            message_type: The message type (e.g., 'config', 'measurements')
            field_name: The field name to get the type for
            
        Returns:
            String representing the data type category, or None if not found
        """
        field_types = self.get_field_types(message_type)
        return field_types.get(field_name)
    
    def _get_protobuf_field_type_category(self, field_desc) -> str:
        """
        Convert protobuf field descriptor type to data type categories.
        Uses google.protobuf.descriptor.FieldDescriptor type constants.
        
        Args:
            field_desc: FieldDescriptor from protobuf message
            
        Returns:
            String representing the data type category
        """
        try:
            from google.protobuf.descriptor import FieldDescriptor
            
            # Handle repeated fields
            if field_desc.label == FieldDescriptor.LABEL_REPEATED:
                return 'array'
            
            # Map protobuf types to our categories
            type_mapping = {
                FieldDescriptor.TYPE_DOUBLE: 'integer',
                FieldDescriptor.TYPE_FLOAT: 'integer', 
                FieldDescriptor.TYPE_INT64: 'integer',
                FieldDescriptor.TYPE_UINT64: 'integer',
                FieldDescriptor.TYPE_INT32: 'integer',
                FieldDescriptor.TYPE_FIXED64: 'integer',
                FieldDescriptor.TYPE_FIXED32: 'integer',
                FieldDescriptor.TYPE_BOOL: 'boolean',
                FieldDescriptor.TYPE_STRING: 'string',
                FieldDescriptor.TYPE_BYTES: 'bytes',
                FieldDescriptor.TYPE_UINT32: 'integer',
                FieldDescriptor.TYPE_ENUM: 'enum',
                FieldDescriptor.TYPE_SFIXED32: 'integer',
                FieldDescriptor.TYPE_SFIXED64: 'integer',
                FieldDescriptor.TYPE_SINT32: 'integer',
                FieldDescriptor.TYPE_SINT64: 'integer',
                FieldDescriptor.TYPE_MESSAGE: 'object'
            }
            
            return type_mapping.get(field_desc.type, 'string')
            
        except ImportError:
            logger.warning("google.protobuf.descriptor not available for field type detection")
            return 'unknown'
        except Exception as e:
            logger.warning(f"Error getting protobuf field type for {field_desc.name}: {e}")
            return 'unknown'

    def parse_dict_to_message(self, data: Dict[str, Any], message_type: str) -> bytes:
        """
        Convert dictionary data to protobuf message bytes.
        Generic function that works with any protobuf message type.

        Args:
            data: Dictionary containing fields to encode
            message_type: Type of protobuf message to create

        Returns:
            bytes: Serialized protobuf message

        Raises:
            ValueError: If message_type is not available
            RuntimeError: If encoding fails
        """
        logger.debug(f"Converting dictionary to {message_type} protobuf message")
        
        if message_type not in self.proto_modules:
            available_types = list(self.proto_modules.keys())
            raise ValueError(f"Message type '{message_type}' not available for {self.manufacturer}. Available types: {available_types}")

        proto_info = self.proto_modules[message_type]
        proto_class = proto_info['class']

        # Create instance of the protobuf class
        proto_message = proto_class()

        # Generic field assignment - no manufacturer-specific logic
        for field_name, value in data.items():
            if hasattr(proto_message, field_name):
                try:
                    # Handle special cases for different field types
                    if isinstance(value, str) and field_name in ['serial_number', 'encryption_key'] and hasattr(proto_message, field_name):
                        # Convert hex string to bytes for binary fields
                        try:
                            setattr(proto_message, field_name, bytes.fromhex(value))
                        except ValueError:
                            # If not valid hex, set as string
                            setattr(proto_message, field_name, value.encode('utf-8'))
                    elif isinstance(value, list):
                        # Handle repeated fields
                        field_attr = getattr(proto_message, field_name)
                        if hasattr(field_attr, 'extend'):
                            field_attr.extend(value)
                        elif hasattr(field_attr, 'append'):
                            for item in value:
                                field_attr.append(item)
                        else:
                            setattr(proto_message, field_name, value)
                    else:
                        setattr(proto_message, field_name, value)
                    
                    logger.debug(f"Set field {field_name} = {value}")
                except Exception as e:
                    logger.warning(f"Failed to set field {field_name} = {value}: {e}")
                    continue
            else:
                logger.debug(f"Field {field_name} not found in {message_type} message")

        # Serialize to bytes
        try:
            serialized_data = proto_message.SerializeToString()
            logger.debug(f"Successfully serialized {message_type} message to {len(serialized_data)} bytes")
            return serialized_data
        except Exception as e:
            logger.error(f"Failed to serialize {message_type} message: {e}")
            raise RuntimeError(f"Protobuf serialization failed for {message_type}: {e}") from e

    def cleanup(self):
        """Clean up compiler resources."""
        if self.compiler:
            self.compiler.cleanup()
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.cleanup() 