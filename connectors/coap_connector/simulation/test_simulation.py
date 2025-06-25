#!/usr/bin/env python3
"""
Quick test script to verify simulation fixes
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_payload_generation():
    """Test that payload generation works with correct parameters"""
    print("🧪 Testing payload generation...")
    
    try:
        from test_efento_parser import generate_realistic_measurement_payload
        
        # Test with correct parameters
        payload_data = generate_realistic_measurement_payload(
            device_serial="282c0242526f",
            include_errors=False,
            measurement_types=["TEMPERATURE", "HUMIDITY"]
        )
        
        print(f"✅ Payload generation successful!")
        print(f"   Payload size: {len(payload_data['payload_bytes'])} bytes")
        print(f"   Payload type: {type(payload_data['payload_bytes'])}")
        print(f"   Payload hex: {payload_data['payload_bytes'][:20].hex()}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Payload generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_coap_client_detection():
    """Test CoAP client detection"""
    print("\n🔍 Testing CoAP client detection...")
    
    try:
        from simulate_efento_sensor import EfentoSensorSimulator
        
        simulator = EfentoSensorSimulator()
        client = simulator._find_coap_client()
        
        if client:
            print(f"✅ CoAP client detected: {client}")
            return True
        else:
            print("❌ No CoAP client found")
            return False
            
    except Exception as e:
        print(f"❌ CoAP client detection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_basic_simulation():
    """Test basic simulation functionality"""
    print("\n🚀 Testing basic simulation...")
    
    try:
        from simulate_efento_sensor import EfentoSensorSimulator
        
        simulator = EfentoSensorSimulator()
        
        # Test sensor reading simulation
        status = simulator._simulate_sensor_readings()
        print(f"✅ Sensor reading simulation: {status}")
        print(f"   Temperature: {simulator.temperature:.1f}°C")
        print(f"   Humidity: {simulator.humidity:.1f}%")
        
        # Test payload generation
        payload = simulator._generate_measurement_payload()
        print(f"✅ Measurement payload generated: {len(payload)} bytes")
        print(f"   Payload hex: {payload[:20].hex()}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Basic simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("🔧 Simulation Fix Verification")
    print("=" * 40)
    
    tests = [
        ("Payload Generation", test_payload_generation),
        ("CoAP Client Detection", test_coap_client_detection),
        ("Basic Simulation", test_basic_simulation)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            results.append((test_name, False))
    
    print("\n📊 Test Results:")
    print("=" * 40)
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Summary: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Simulation should work correctly.")
    else:
        print("⚠️  Some tests failed. Check the errors above.")
    
    return passed == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 