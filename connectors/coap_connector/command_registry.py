import logging
import time
import importlib.util
import sys
from typing import Dict, Any, Optional, Tuple, Callable
import asyncio
import os.path

logger = logging.getLogger(__name__)

class CommandRegistry:
    """
    Maps device IDs to their parser scripts and handles command formatting.
    Responsible for loading parser scripts dynamically and providing access to their format_command function.
    """
    def __init__(self):
        """
        Initialize the command registry.
        """
        self.parsers = {}  # device_id -> parser_module
        
    async def register_device(self, device_id: str, parser_script_path: str) -> bool:
        """
        Register a device with its parser script.
        
        Args:
            device_id: The unique identifier of the device
            parser_script_path: Path to the parser script relative to the storage directory
            
        Returns:
            True if registration was successful, False otherwise
        """
        try:
            parser_module = await self._load_script(parser_script_path)
            if parser_module:
                self.parsers[device_id] = parser_module
                logger.info(f"Registered device {device_id} with parser {parser_script_path}")
                return True
            else:
                logger.error(f"Failed to load parser script for device {device_id}: {parser_script_path}")
                return False
        except Exception as e:
            logger.error(f"Error registering device {device_id} with parser {parser_script_path}: {e}")
            return False
            
    async def _load_script(self, script_path: str) -> Optional[Any]:
        """
        Load a parser script by path.
        
        Args:
            script_path: Path to the parser script relative to the storage directory
            
        Returns:
            The loaded module or None if loading fails
        """
        try:
            # Construct the absolute path to the script
            # Assuming the script is in a standard location like 'storage/parser_scripts/'
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            storage_dir = os.path.join(base_dir, 'storage', 'parser_scripts')
            abs_path = os.path.join(storage_dir, script_path) if not os.path.isabs(script_path) else script_path
            
            # Check if the script exists
            if not os.path.exists(abs_path):
                logger.error(f"Parser script not found: {abs_path}")
                return None
                
            # Use importlib to load the module
            module_name = os.path.splitext(os.path.basename(abs_path))[0]
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            if spec is None:
                logger.error(f"Failed to create spec for module: {abs_path}")
                return None
                
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            # Verify that the module has the required format_command function
            if not hasattr(module, 'format_command'):
                logger.error(f"Parser script {abs_path} does not have a format_command function")
                return None
                
            return module
        except Exception as e:
            logger.exception(f"Error loading parser script {script_path}: {e}")
            return None
            
    def is_registered(self, device_id: str) -> bool:
        """
        Check if a device is registered with a parser.
        
        Args:
            device_id: The device ID to check
            
        Returns:
            True if the device is registered, False otherwise
        """
        return device_id in self.parsers
            
    async def format_command(self, device_id: str, command: dict, config: dict) -> Optional[bytes]:
        """
        Format a command for a specific device using its parser.
        Will attempt to register device if not already registered and parser_script_ref is in command or config.
        
        Args:
            device_id: The device ID the command is for
            command: The command to format (may contain 'parser_script_ref' field with parser script reference)
            config: Device configuration (may contain 'parser_script_ref' field)
            
        Returns:
            The formatted command as bytes or None if formatting fails
        """
        # Get parser script reference from command parser_script_ref field first, then from config
        parser_script_ref = command.get('parser_script_ref')
        
        if not parser_script_ref:
            logger.error(f"No parser script reference found in command or config for device {device_id}")
            return None
        
        # Check if parser is already registered and cached
        if device_id not in self.parsers:
            # Parser not found, register it with the parser script reference
            try:
                success = await self.register_device(device_id, parser_script_ref)
                if not success:
                    logger.error(f"Failed to register device {device_id} with parser {parser_script_ref}")
                    return None
            except Exception as e:
                logger.error(f"Failed to register device {device_id}: {e}")
                return None
                
        # Now format the command using the cached parser
        try:
            parser = self.parsers.get(device_id)
            if not parser or not hasattr(parser, 'format_command'):
                logger.error(f"Parser for device {device_id} is invalid or does not have format_command function")
                return None
                
            # Call the format_command function with command and config
            return parser.format_command(command, config)
        except Exception as e:
            logger.exception(f"Error formatting command for device {device_id}: {e}")
            return None 