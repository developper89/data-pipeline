# shared/script_client.py
import importlib.util
import logging
import sys
import os

logger = logging.getLogger(__name__)

class ScriptNotFoundError(Exception):
    """Raised when a requested parser script cannot be found."""
    pass

class ScriptClient:
    """
    Client for loading and managing parser scripts from various storage backends.
    Supports local file system and can be extended for cloud storage (S3, etc.).
    """
    
    def __init__(self, storage_type: str, local_dir: str = None):
        """
        Initialize the script client.
        
        Args:
            storage_type: Type of storage backend ('local', 's3', etc.)
            local_dir: Directory path for local storage (required for 'local' type)
        """
        self.storage_type = storage_type.lower()
        self.local_dir = local_dir
        
        if self.storage_type == 'local':
            if not local_dir:
                raise ValueError("LOCAL_SCRIPT_DIR must be set for local storage type")
            # Ensure base directory exists for writing/reading
            os.makedirs(self.local_dir, exist_ok=True)
            logger.info(f"Using local script storage in directory: {local_dir}")
        elif self.storage_type != 's3':  # Modify if adding more types
            raise ValueError(f"Unsupported SCRIPT_STORAGE_TYPE: {storage_type}")

    async def get_module(self, script_ref: str):
        """
        Load and return a Python module from the given script reference.
        
        Args:
            script_ref: Path or reference to the script file
            
        Returns:
            The loaded Python module
            
        Raises:
            ScriptNotFoundError: If the script cannot be found or loaded
        """
        try:
            spec = importlib.util.spec_from_file_location("dynamic_parser", script_ref)
            if spec is None:
                raise ScriptNotFoundError(f"Could not create module spec for {script_ref}")
                
            script_module = importlib.util.module_from_spec(spec)
            sys.modules["dynamic_parser"] = script_module
            spec.loader.exec_module(script_module)
            return script_module
        except Exception as e:
            logger.error(f"Failed to load script module from {script_ref}: {e}")
            raise ScriptNotFoundError(f"Failed to load script: {script_ref}") from e 