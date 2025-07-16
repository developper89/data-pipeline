"""
Configuration loader utilities for connectors.

This module provides utilities to load and parse connector configurations,
specifically for extracting translator configurations from the YAML config file.
"""
import os
import yaml
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

CONFIG_FILE_PATH = "/app/shared/connectors_config.yaml"


def load_connectors_config() -> Optional[Dict[str, Any]]:
    """Load the connectors configuration from the YAML file."""
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            config_data = yaml.safe_load(f)
            logger.debug(f"Successfully loaded connector config from: {CONFIG_FILE_PATH}")
            return config_data
    except FileNotFoundError:
        logger.error(f"Config file not found at: {CONFIG_FILE_PATH}")
        return None
    except Exception as e:
        logger.error(f"Failed to load config from {CONFIG_FILE_PATH}: {e}")
        return None


def get_connector_config(connector_id: str) -> Optional[Dict[str, Any]]:
    """Get configuration for a specific connector by ID."""
    config_data = load_connectors_config()
    if not config_data:
        return None
    
    connectors = config_data.get('connectors', [])
    for connector in connectors:
        if connector.get('connector_id') == connector_id:
            logger.debug(f"Found configuration for connector: {connector_id}")
            return connector
    
    logger.warning(f"Connector '{connector_id}' not found in configuration")
    return None


def get_translator_configs(connector_id: str) -> List[Dict[str, Any]]:
    """
    Get translator configurations for a specific connector.
    
    Args:
        connector_id: The connector ID to get configurations for
        
    Returns:
        List of translator configurations or empty list if not found
    """
    config_data = load_connectors_config()
    if not config_data:
        return []
        
    connectors = config_data.get('connectors', [])
    
    # Find the connector with the specified ID
    for connector in connectors:
        if connector.get('connector_id') == connector_id:
            translators = connector.get('translators', [])
            logger.debug(f"Found {len(translators)} translator config(s) for connector: {connector_id}")
            return translators
    
    logger.warning(f"No connector found with ID: {connector_id}")
    return []


def get_connector_env(connector_id: str) -> Dict[str, Any]:
    """Get environment variables for a specific connector."""
    connector_config = get_connector_config(connector_id)
    if not connector_config:
        return {}
    
    return connector_config.get('env', {})


def validate_translator_config(config: Dict[str, Any]) -> bool:
    """
    Validate a translator configuration dictionary.
    
    Args:
        config: Translator configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ['type', 'config']
    
    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required field '{field}' in translator config")
            return False
    
    translator_type = config.get('type')
    if not isinstance(translator_type, str):
        logger.error(f"Translator type must be a string, got: {type(translator_type)}")
        return False
    
    translator_config = config.get('config')
    if not isinstance(translator_config, dict):
        logger.error(f"Translator config must be a dictionary, got: {type(translator_config)}")
        return False
    
    return True 