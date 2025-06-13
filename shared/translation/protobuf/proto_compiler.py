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
        
        # First pass: Create module specs for all files
        module_specs = {}
        for pb2_file in pb2_files:
            module_name = pb2_file.stem  # Remove .py extension
            spec = importlib.util.spec_from_file_location(module_name, pb2_file)
            module_specs[module_name] = (spec, pb2_file)
        
        # Second pass: Load all modules and add to sys.modules first
        for module_name, (spec, pb2_file) in module_specs.items():
            try:
                module = importlib.util.module_from_spec(spec)
                # Add to sys.modules BEFORE executing to handle circular imports
                sys.modules[module_name] = module
                logger.debug(f"Pre-registered module in sys.modules: {module_name}")
            except Exception as e:
                logger.warning(f"Failed to create module spec for {module_name}: {e}")
        
        # Third pass: Execute all modules now that they're all registered
        for module_name, (spec, pb2_file) in module_specs.items():
            try:
                if module_name in sys.modules:
                    module = sys.modules[module_name]
                    spec.loader.exec_module(module)
                    self.compiled_modules[module_name] = module
                    logger.debug(f"Successfully executed and loaded module: {module_name}")
                else:
                    logger.warning(f"Module {module_name} not found in sys.modules during execution")
                    
            except Exception as e:
                logger.warning(f"Failed to execute module {module_name}: {e}")
                
                # If it's an import error, show more details
                if isinstance(e, (ImportError, ModuleNotFoundError)):
                    logger.debug(f"Import error details for {module_name}:")
                    logger.debug(f"  Available modules in sys.modules: {list(sys.modules.keys())}")
                    logger.debug(f"  Compiled modules so far: {list(self.compiled_modules.keys())}")
                    
                    # Try to show the problematic import line
                    try:
                        with open(pb2_file, 'r') as f:
                            lines = f.readlines()
                            for i, line in enumerate(lines[:20]):  # Check first 20 lines
                                if 'import' in line and '_pb2' in line:
                                    logger.debug(f"  Line {i+1}: {line.strip()}")
                    except Exception:
                        pass
                
                # Remove from sys.modules if execution failed
                if module_name in sys.modules:
                    del sys.modules[module_name]
    
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
        """Clean up temporary files and remove modules from sys.modules."""
        # Remove compiled modules from sys.modules
        for module_name in list(self.compiled_modules.keys()):
            if module_name in sys.modules:
                del sys.modules[module_name]
                logger.debug(f"Removed module from sys.modules: {module_name}")
        
        # Clear our compiled modules dict
        self.compiled_modules.clear()
        
        # Remove temporary directory
        if self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            try:
                shutil.rmtree(self._temp_dir)
                logger.debug(f"Cleaned up temporary directory: {self._temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temporary directory: {e}")
            finally:
                self._temp_dir = None
    
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