#!/usr/bin/env python3
"""
Test what happens when read_all is called without existing metadata readings.
This simulates the real issue you're experiencing.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from light_controller_parser import parse, UnifiedReadingManager, LightProtocolDecoder
import json

def test_read_all_without_existing_metadata():
    """Test read_all when metadata doesn't contain existing readings."""
    print("=== Testing READ_ALL Without Existing Metadata ===")
    
    # Metadata WITHOUT existing readings (this is likely what's happening)
    metadata = {
        'device_id': '2121004',
        'protocol': 'mqtt',
        # NO 'readings' key - this forces creation of fresh reading
    }
    
    print("=== Metadata state ===")
    print(f"Has readings: {'readings' in metadata}")
    
    # Simulate read_all response with task_count = 7
    packet_data = {
        'id_capteur': 2121004,
        'epoch': 1753453651,
        'header': {
            'task_id': 0,
            'reserved': 0,
            'all': 1,      # read_all
            'clear': 0,
            'get_set': 0   # read operation
        },
        'alarm': {
            'time_beg': 0,
            'duration': 0,
            'day': 0,
            'task_id': 7,  # task_count = 7
            'channel': 0,
            'active': 0,
            'run_once': 0
        }
    }
    
    # Encode packet to hex
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")  
    print(f"Hex payload for read_all: {hex_payload}")
    
    # Parse the payload - this will create a fresh reading
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        
        print("\n=== Result with Fresh Reading ===")
        print(f"Task 0 active: {reading['values'][4][0]}")       # Will be False (default)
        print(f"Task 0 time_beg: {reading['values'][0][0]}")     # Will be 0 (default)
        print(f"Task count: {reading['values'][11]}")            # Will be 7 (from device)
        print(f"Operation: {reading['metadata']['operation']}")  # Will be 'read_all'
        
        # This explains the issue!
        print("\n🔍 DIAGNOSIS:")
        if reading['values'][4][0] == False and reading['values'][0][0] == 0:
            print("❌ This reproduces your issue! Task data is reset to defaults.")
            print("💡 Root cause: metadata.readings is not being passed between operations")
        
        # Show what a fresh reading looks like
        print("\n=== Fresh Reading Structure ===")
        print("All task values are defaults:")
        print(f"  task_active: {reading['values'][4][:3]}... (all False)")
        print(f"  task_time_beg: {reading['values'][0][:3]}... (all 0)")
        print(f"  task_count: {reading['values'][11]} (from device)")
        
    else:
        print("❌ READ_ALL test failed - no result")

if __name__ == "__main__":
    try:
        test_read_all_without_existing_metadata()
        print("\n🎯 This test confirms the root cause of your issue!")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()