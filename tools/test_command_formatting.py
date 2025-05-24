#!/usr/bin/env python3
"""
Test script for the bidirectional parser command formatting.
This script simulates the command formatting process by:
1. Loading a parser script
2. Formatting a command for a device
3. Printing the formatted command

Usage:
    python3 test_command_formatting.py <device_id> <parser_script_path> <command_type>

Example:
    python3 test_command_formatting.py test_device efento_bidirectional_parser.py config_update
"""

import sys
import os
import asyncio
import json
import base64
import logging
import time
import importlib.util
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("test_command_formatting")

async def load_parser_script(script_path: str) -> Optional[Any]:
    """
    Load a parser script by path.
    
    Args:
        script_path: Path to the parser script
        
    Returns:
        The loaded module or None if loading fails
    """
    try:
        # Construct the absolute path to the script
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        storage_dir = os.path.join(base_dir, 'storage', 'parser_scripts')
        abs_path = os.path.join(storage_dir, script_path) if not os.path.isabs(script_path) else script_path
        
        logger.info(f"Loading parser script from: {abs_path}")
        
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

def get_test_command(device_id: str, command_type: str) -> Dict[str, Any]:
    """
    Get a test command for the given device and command type.
    
    Args:
        device_id: The device ID
        command_type: The command type (e.g., config_update, time_sync)
        
    Returns:
        A command dictionary
    """
    if command_type == "config_update":
        return {
            "command_type": "config_update",
            "device_id": device_id,
            "request_id": f"test-{int(time.time())}",
            "payload": {
                "measurement_interval": 900,  # 15 minutes in seconds
                "transmission_interval": 3600,  # 1 hour in seconds
                "ack_interval": "always",
                "server_address": "192.168.1.100",
                "server_port": 5683,
                "request_device_info": True,
                "rules": [
                    {
                        "type": "above_threshold",
                        "channel": 1,
                        "threshold": 25.0,
                        "hysteresis": 2.0,
                        "action": "trigger_transmission"
                    }
                ]
            }
        }
    elif command_type == "time_sync":
        return {
            "command_type": "time_sync",
            "device_id": device_id,
            "request_id": f"test-{int(time.time())}",
            "payload": {}
        }
    else:
        return {
            "command_type": command_type,
            "device_id": device_id,
            "request_id": f"test-{int(time.time())}",
            "payload": {
                "param1": "value1",
                "param2": "value2"
            }
        }

def get_test_device_config(device_id: str) -> Dict[str, Any]:
    """
    Get a test device configuration.
    
    Args:
        device_id: The device ID
        
    Returns:
        A device configuration dictionary
    """
    return {
        "device_id": device_id,
        "parser_script_ref": "efento_bidirectional_parser.py",
        "labels": {
            "location": "test-location",
            "type": "test-device"
        },
        "metadata": {
            "uri_path": "/m"  # Default path for measurement data
        }
    }

async def main():
    """Main function."""
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <device_id> <parser_script_path> <command_type>")
        print(f"Example: {sys.argv[0]} test_device efento_bidirectional_parser.py config_update")
        return
        
    device_id = sys.argv[1]
    parser_script_path = sys.argv[2]
    command_type = sys.argv[3]
    
    # Load the parser script
    parser_module = await load_parser_script(parser_script_path)
    if not parser_module:
        logger.error(f"Failed to load parser script: {parser_script_path}")
        return
        
    # Get test command and device configuration
    command = get_test_command(device_id, command_type)
    device_config = get_test_device_config(device_id)
    
    # Format the command
    try:
        logger.info(f"Formatting command: {command}")
        formatted_command = parser_module.format_command(command, device_config)
        
        logger.info(f"Command formatted successfully, binary length: {len(formatted_command)} bytes")
        logger.info(f"Command hex: {formatted_command.hex()}")
        
        # If it's a protobuf message, we can't decode it directly
        # But for time_sync, we can at least show the timestamp
        if command_type == "time_sync":
            timestamp = int(formatted_command.hex(), 16)
            logger.info(f"Decoded time command: timestamp={timestamp} ({time.ctime(timestamp)})")
        
    except Exception as e:
        logger.exception(f"Error formatting command: {e}")
        return
        
    logger.info("Command formatting test completed successfully")

if __name__ == "__main__":
    asyncio.run(main()) 