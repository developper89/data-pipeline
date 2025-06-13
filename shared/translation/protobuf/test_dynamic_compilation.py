#!/usr/bin/env python3
"""
Test script for dynamic protobuf compilation.

This script tests the dynamic compilation of .proto files to verify
the system works correctly before using it in the translation layer.
"""
import sys
import os

# Add the app directory to the path
sys.path.insert(0, '/app')

from shared.translation.protobuf.proto_compiler import ProtobufCompiler, check_protoc_available

def test_protoc_availability():
    """Test if protoc compiler is available."""
    print("🔍 Testing protoc availability...")
    
    if check_protoc_available():
        print("✅ protoc compiler is available")
        return True
    else:
        print("❌ protoc compiler not found")
        return False

def test_efento_compilation():
    """Test compilation of Efento protobuf files."""
    print("\n🔍 Testing Efento protobuf compilation...")
    
    try:
        # Create compiler for Efento
        compiler = ProtobufCompiler("efento")
        
        # Compile proto files
        compiled_modules = compiler.compile_proto_files()
        
        if compiled_modules:
            print(f"✅ Successfully compiled {len(compiled_modules)} modules:")
            for module_name in compiled_modules.keys():
                print(f"   - {module_name}")
            
            # Test getting specific classes
            test_classes = [
                ("proto_measurements_pb2", "ProtoMeasurements"),
                ("proto_device_info_pb2", "ProtoDeviceInfo"),
                ("proto_config_pb2", "ProtoConfig"),
            ]
            
            for module_name, class_name in test_classes:
                cls = compiler.get_proto_class(module_name, class_name)
                if cls:
                    print(f"✅ Found class {class_name} in {module_name}")
                    
                    # Test creating an instance
                    try:
                        instance = cls()
                        print(f"✅ Successfully created instance of {class_name}")
                    except Exception as e:
                        print(f"❌ Failed to create instance of {class_name}: {e}")
                else:
                    print(f"❌ Class {class_name} not found in {module_name}")
            
            # Cleanup
            compiler.cleanup()
            print("✅ Cleanup completed")
            return True
            
        else:
            print("❌ No modules were compiled")
            return False
            
    except Exception as e:
        print(f"❌ Compilation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_message_parser():
    """Test the updated message parser with dynamic compilation."""
    print("\n🔍 Testing ProtobufMessageParser with dynamic compilation...")
    
    try:
        from shared.translation.protobuf.message_parser import ProtobufMessageParser
        
        # Create message types config (same as in connector config)
        message_types = {
            "measurements": {
                "proto_class": "ProtoMeasurements",
                "proto_module": "proto_measurements_pb2",
                "required_fields": ["serial_num", "battery_status"]
            },
            "device_info": {
                "proto_class": "ProtoDeviceInfo",
                "proto_module": "proto_device_info_pb2",
                "required_fields": ["serial_number"]
            },
            "config": {
                "proto_class": "ProtoConfig",
                "proto_module": "proto_config_pb2",
                "required_fields": []
            }
        }
        
        # Create parser
        parser = ProtobufMessageParser("efento", message_types)
        
        if parser.proto_modules:
            print(f"✅ ProtobufMessageParser loaded {len(parser.proto_modules)} message types")
            for msg_type in parser.proto_modules.keys():
                print(f"   - {msg_type}")
        else:
            print("❌ ProtobufMessageParser failed to load any message types")
            return False
            
        # Cleanup
        parser.cleanup()
        print("✅ Parser cleanup completed")
        return True
        
    except Exception as e:
        print(f"❌ Message parser test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("🚀 Starting Dynamic Protobuf Compilation Tests\n")
    
    success = True
    
    # Test 1: protoc availability
    if not test_protoc_availability():
        success = False
    
    # Test 2: Efento compilation
    if not test_efento_compilation():
        success = False
    
    # Test 3: Message parser
    if not test_message_parser():
        success = False
    
    print("\n" + "="*60)
    if success:
        print("🎉 All tests passed! Dynamic protobuf compilation is working!")
    else:
        print("❌ Some tests failed. Check the output above for details.")
    print("="*60)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 