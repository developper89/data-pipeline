"""
Dynamic protobuf compiler for translation layer.

This module handles compiling .proto files to Python modules at runtime,
avoiding the need for pre-compiled protobuf files.
"""
import os
import sys
import tempfile
import subprocess
import importlib.util
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class ProtobufCompiler:
    """Dynamically compiles .proto files to Python modules."""
    
    def __init__(self, manufacturer: str):
        self.manufacturer = manufacturer
        self.proto_dir = Path(f"/app/shared/translation/protobuf/proto_schemas/{manufacturer}")
        self.compiled_modules = {}
        self._temp_dir = None
    
    def compile_proto_files(self) -> Dict[str, Any]:
        """
        Compile all .proto files for this manufacturer to Python modules.
        
        Returns:
            Dictionary mapping module names to compiled Python modules
        """
        if not self.proto_dir.exists():
            logger.error(f"Proto directory not found: {self.proto_dir}")
            return {}
        
        # Find all .proto files
        proto_files = list(self.proto_dir.glob("*.proto"))
        if not proto_files:
            logger.warning(f"No .proto files found in {self.proto_dir}")
            return {}
        
        logger.info(f"Found {len(proto_files)} .proto files for {self.manufacturer}")
        
        # Create temporary directory for compiled files
        self._temp_dir = tempfile.mkdtemp(prefix=f"protobuf_{self.manufacturer}_")
        logger.debug(f"Using temporary directory: {self._temp_dir}")
        
        try:
            # Compile all proto files
            self._compile_proto_files(proto_files)
            
            # Load compiled modules
            self._load_compiled_modules()
            
            logger.info(f"Successfully compiled {len(self.compiled_modules)} protobuf modules for {self.manufacturer}")
            return self.compiled_modules
            
        except Exception as e:
            logger.error(f"Failed to compile protobuf files for {self.manufacturer}: {e}")
            return {}
    
    def _compile_proto_files(self, proto_files):
        """Compile .proto files using protoc."""
        # Build protoc command
        cmd = [
            "protoc",
            f"--python_out={self._temp_dir}",
            f"--proto_path={self.proto_dir}",
        ]
        
        # Add all proto files
        for proto_file in proto_files:
            cmd.append(str(proto_file))
        
        logger.debug(f"Running protoc command: {' '.join(cmd)}")
        
        # Run protoc
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            error_msg = f"protoc compilation failed:\n{result.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        logger.debug(f"protoc compilation successful: {result.stdout}")
    
    def _load_compiled_modules(self):
        """Load compiled Python modules from temporary directory."""
        # Find all compiled _pb2.py files
        pb2_files = list(Path(self._temp_dir).glob("*_pb2.py"))
        
        for pb2_file in pb2_files:
            module_name = pb2_file.stem  # Remove .py extension
            
            try:
                # Load module dynamically
                spec = importlib.util.spec_from_file_location(module_name, pb2_file)
                module = importlib.util.module_from_spec(spec)
                
                # Add to sys.modules to handle cross-references
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                
                self.compiled_modules[module_name] = module
                logger.debug(f"Loaded compiled module: {module_name}")
                
            except Exception as e:
                logger.warning(f"Failed to load compiled module {module_name}: {e}")
    
    def get_proto_class(self, module_name: str, class_name: str) -> Optional[type]:
        """
        Get a protobuf class from a compiled module.
        
        Args:
            module_name: Name of the compiled module (e.g., 'proto_measurements_pb2')
            class_name: Name of the protobuf class (e.g., 'ProtoMeasurements')
            
        Returns:
            The protobuf class or None if not found
        """
        if module_name not in self.compiled_modules:
            logger.error(f"Module {module_name} not found in compiled modules")
            return None
        
        module = self.compiled_modules[module_name]
        
        if not hasattr(module, class_name):
            logger.error(f"Class {class_name} not found in module {module_name}")
            return None
        
        return getattr(module, class_name)
    
    def cleanup(self):
        """Clean up temporary files."""
        if self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            try:
                shutil.rmtree(self._temp_dir)
                logger.debug(f"Cleaned up temporary directory: {self._temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temporary directory: {e}")
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.cleanup()


def check_protoc_available() -> bool:
    """Check if protoc compiler is available."""
    try:
        result = subprocess.run(["protoc", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            logger.debug(f"protoc available: {result.stdout.strip()}")
            return True
        else:
            logger.error("protoc not available")
            return False
    except FileNotFoundError:
        logger.error("protoc command not found")
        return False 