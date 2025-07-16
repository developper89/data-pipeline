"""
Simple protobuf compiler for translation layer.

This module compiles .proto files to a fixed directory structure
and uses standard Python imports.
"""
import os
import subprocess
import importlib
import logging
import sys
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class ProtobufCompiler:
    """Compiles .proto files to a fixed directory structure."""
    
    def __init__(self, manufacturer: str):
        self.manufacturer = manufacturer
        self.proto_schemas_dir = Path(f"/app/shared/translation/protobuf/proto_schemas/{manufacturer}")
        self.proto_output_dir = self.proto_schemas_dir / "protobuf"
        self.compiled_modules = {}
        self._path_added = False
    
    def compile_proto_files(self) -> Dict[str, Any]:
        """
        Compile all .proto files for this manufacturer to the protobuf directory.
        
        Returns:
            Dictionary mapping module names to compiled Python modules
        """
        if not self.proto_schemas_dir.exists():
            logger.error(f"Proto schemas directory not found: {self.proto_schemas_dir}")
            return {}
        
        # Find all .proto files
        proto_files = list(self.proto_schemas_dir.glob("*.proto"))
        if not proto_files:
            logger.warning(f"No .proto files found in {self.proto_schemas_dir}")
            return {}
        
        logger.info(f"Found {len(proto_files)} .proto files for {self.manufacturer}")
        
        # Create output directory
        self.proto_output_dir.mkdir(exist_ok=True)
        
        # Create __init__.py to make it a Python package
        init_file = self.proto_output_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Auto-generated protobuf package\n")
        
        try:
            # Compile all proto files at once (handles dependencies automatically)
            self._compile_proto_files(proto_files)
            
            # Add protobuf output directory to Python path for imports
            self._add_to_python_path()
            
            # Load compiled modules using direct imports
            self._load_compiled_modules()
            
            logger.debug(f"Successfully compiled and loaded {len(self.compiled_modules)} protobuf modules for {self.manufacturer}")
            return self.compiled_modules
            
        except Exception as e:
            logger.error(f"Failed to compile protobuf files for {self.manufacturer}: {e}")
            return {}
    
    def _compile_proto_files(self, proto_files):
        """Compile .proto files using protoc."""
        # Build protoc command - compile all files at once
        cmd = [
            "protoc",
            f"--python_out={self.proto_output_dir}",
            f"--proto_path={self.proto_schemas_dir}",
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
        
        logger.debug(f"protoc compilation successful")
    
    def _add_to_python_path(self):
        """Add protobuf output directory to Python path."""
        protobuf_path = str(self.proto_output_dir)
        if protobuf_path not in sys.path:
            sys.path.insert(0, protobuf_path)
            self._path_added = True
            logger.debug(f"Added {protobuf_path} to Python path")
    
    def _load_compiled_modules(self):
        """Load compiled protobuf modules."""
        # Find all compiled .py files
        py_files = list(self.proto_output_dir.glob("*_pb2.py"))
        
        if not py_files:
            logger.warning(f"No compiled protobuf modules found in {self.proto_output_dir}")
            return
        
        logger.debug(f"Found {len(py_files)} compiled protobuf modules")
        
        # Load each module
        for py_file in py_files:
            module_name = py_file.stem  # Remove .py extension
            
            try:
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec is None:
                    logger.warning(f"Could not create spec for {module_name}")
                    continue
                    
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                self.compiled_modules[module_name] = module
                logger.debug(f"Loaded compiled module: {module_name}")
                
            except Exception as e:
                logger.warning(f"Failed to load compiled module {module_name}: {e}")
                continue
    
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
        """Remove protobuf directory from Python path if we added it."""
        if self._path_added and sys is not None:
            protobuf_path = str(self.proto_output_dir)
            try:
                sys.path.remove(protobuf_path)
                self._path_added = False
                logger.debug(f"Removed {protobuf_path} from Python path")
            except (ValueError, AttributeError):
                # Path wasn't in sys.path or sys is None during shutdown
                pass
    
    def __del__(self):
        """Clean up on destruction."""
        self.cleanup()

    def try_load_existing_modules(self) -> bool:
        """
        Try to load existing compiled protobuf modules.
        
        Returns:
            True if existing modules were successfully loaded, False otherwise
        """
        if not self.proto_output_dir.exists():
            logger.debug(f"Protobuf output directory does not exist: {self.proto_output_dir}")
            return False
        
        # Check if there are existing compiled modules
        py_files = list(self.proto_output_dir.glob("*_pb2.py"))
        if not py_files:
            logger.debug(f"No existing compiled protobuf modules found in {self.proto_output_dir}")
            return False
        
        logger.debug(f"Found {len(py_files)} existing compiled protobuf modules")
        
        try:
            # Add to Python path
            self._add_to_python_path()
            
            # Load compiled modules
            self._load_compiled_modules()
            
            if self.compiled_modules:
                logger.debug(f"Successfully loaded {len(self.compiled_modules)} existing compiled modules for {self.manufacturer}")
                return True
            else:
                logger.debug(f"No modules could be loaded for {self.manufacturer}")
                return False
                
        except Exception as e:
            logger.debug(f"Failed to load existing compiled modules for {self.manufacturer}: {e}")
            return False


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