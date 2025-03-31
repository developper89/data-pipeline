# normalizer/script_client.py
import importlib
import logging
import sys
import os

logger = logging.getLogger(__name__)

class ScriptNotFoundError(Exception):
    pass

class ScriptClient:
    def __init__(self, storage_type: str, local_dir: str = None):
        self.storage_type = storage_type.lower()
        self.local_dir = local_dir
        if self.storage_type == 'local':
             if not local_dir:
                  raise ValueError("LOCAL_SCRIPT_DIR must be set for local storage type")
             # Ensure base directory exists for writing/reading
             os.makedirs(self.local_dir, exist_ok=True)
             logger.info(f"Using local script storage in directory: {local_dir}")
        elif self.storage_type != 's3': # Modify if adding more types
             raise ValueError(f"Unsupported SCRIPT_STORAGE_TYPE: {storage_type}")

    async def get_module(self, script_ref: str) -> str:
        spec = importlib.util.spec_from_file_location("dynamic_parser", script_ref)
        script_module = importlib.util.module_from_spec(spec)
        sys.modules["dynamic_parser"] = script_module
        spec.loader.exec_module(script_module)
        return script_module
