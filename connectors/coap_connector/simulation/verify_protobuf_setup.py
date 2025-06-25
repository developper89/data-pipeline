#!/usr/bin/env python3
"""
Protobuf Setup Verification Script

This script verifies that protobuf modules are properly set up in the container
environment, matching the approach used by the translation layer.
"""

import os
import sys
from pathlib import Path

def check_protobuf_setup():
    """Check protobuf setup using the same approach as the translator."""
    print("🔍 Protobuf Setup Verification")
    print("=" * 40)
    
    # Check protobuf package availability
    try:
        import google.protobuf
        print(f"✅ protobuf package available: {google.protobuf.__version__}")
    except ImportError:
        print("❌ protobuf package not available")
        return False
    
    # Check for protoc compiler
    import subprocess
    try:
        result = subprocess.run(['protoc', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ protoc compiler available: {result.stdout.strip()}")
        else:
            print("❌ protoc compiler not available")
    except FileNotFoundError:
        print("❌ protoc compiler not found in PATH")
    
    # Check container directory structure
    efento_proto_dir = Path("/app/shared/translation/protobuf/proto_schemas/efento")
    efento_compiled_dir = efento_proto_dir / "protobuf"
    
    print(f"\n📁 Directory Structure:")
    print(f"   Proto schemas: {efento_proto_dir}")
    if efento_proto_dir.exists():
        print(f"   ✅ EXISTS")
        proto_files = list(efento_proto_dir.glob("*.proto"))
        print(f"   📄 Proto files: {len(proto_files)}")
        for proto_file in proto_files:
            print(f"      - {proto_file.name}")
    else:
        print(f"   ❌ NOT FOUND")
        return False
    
    print(f"\n   Compiled modules: {efento_compiled_dir}")
    if efento_compiled_dir.exists():
        print(f"   ✅ EXISTS")
        pb2_files = list(efento_compiled_dir.glob("*_pb2.py"))
        print(f"   🐍 Python modules: {len(pb2_files)}")
        for pb2_file in pb2_files:
            print(f"      - {pb2_file.name}")
    else:
        print(f"   ❌ NOT FOUND - needs compilation")
    
    # Test the compiler approach
    print(f"\n🔧 Testing ProtobufCompiler:")
    try:
        # Add shared to path
        shared_path = "/app/shared"
        if shared_path not in sys.path:
            sys.path.insert(0, shared_path)
        
        from translation.protobuf.proto_compiler import ProtobufCompiler
        
        # Create compiler for efento
        compiler = ProtobufCompiler("efento")
        print(f"✅ ProtobufCompiler created")
        
        # Try loading existing modules
        if compiler.try_load_existing_modules():
            print(f"✅ Existing modules loaded successfully")
            
            # Test getting a specific class
            measurements_class = compiler.get_proto_class("proto_measurements_pb2", "ProtoMeasurements")
            if measurements_class:
                print(f"✅ ProtoMeasurements class available")
                
                # Test creating an instance
                try:
                    instance = measurements_class()
                    print(f"✅ ProtoMeasurements instance created")
                    return True
                except Exception as e:
                    print(f"❌ Failed to create ProtoMeasurements instance: {e}")
                    return False
            else:
                print(f"❌ ProtoMeasurements class not available")
                return False
        else:
            print(f"⚠️  No existing modules, attempting compilation")
            
            # Try compiling
            compiled_modules = compiler.compile_proto_files()
            if compiled_modules:
                print(f"✅ Compilation successful: {len(compiled_modules)} modules")
                return True
            else:
                print(f"❌ Compilation failed")
                return False
                
    except ImportError as e:
        print(f"❌ Failed to import ProtobufCompiler: {e}")
        return False
    except Exception as e:
        print(f"❌ ProtobufCompiler test failed: {e}")
        return False

def test_direct_import():
    """Test direct import of compiled protobuf modules."""
    print(f"\n🧪 Testing Direct Import:")
    
    # Add compiled protobuf directory to path
    compiled_dir = "/app/shared/translation/protobuf/proto_schemas/efento/protobuf"
    if os.path.exists(compiled_dir):
        sys.path.insert(0, compiled_dir)
        print(f"✅ Added {compiled_dir} to Python path")
        
        # Try importing compiled modules
        modules_to_test = [
            "proto_measurements_pb2",
            "proto_device_info_pb2", 
            "proto_config_pb2"
        ]
        
        for module_name in modules_to_test:
            try:
                module = __import__(module_name)
                print(f"✅ {module_name} imported successfully")
                
                # Test specific classes
                if module_name == "proto_measurements_pb2":
                    if hasattr(module, 'ProtoMeasurements'):
                        cls = getattr(module, 'ProtoMeasurements')
                        instance = cls()
                        print(f"✅ ProtoMeasurements instance created via direct import")
                    else:
                        print(f"❌ ProtoMeasurements class not found in {module_name}")
                        
            except ImportError as e:
                print(f"❌ Failed to import {module_name}: {e}")
            except Exception as e:
                print(f"❌ Error testing {module_name}: {e}")
    else:
        print(f"❌ Compiled directory not found: {compiled_dir}")

if __name__ == '__main__':
    print("🚀 Protobuf Environment Verification")
    print("=" * 50)
    
    # Show environment info
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python version: {sys.version}")
    print(f"Python path entries: {len(sys.path)}")
    
    # Run checks
    setup_ok = check_protobuf_setup()
    test_direct_import()
    
    if setup_ok:
        print(f"\n🎉 Protobuf setup verification PASSED")
        print("The container environment is ready for protobuf-based testing!")
    else:
        print(f"\n❌ Protobuf setup verification FAILED")
        print("Container environment needs configuration.")
        sys.exit(1) 