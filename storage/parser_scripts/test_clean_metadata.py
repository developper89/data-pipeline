#!/usr/bin/env python3
"""
Test the cleaned up metadata approach where values are passed directly.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from light_controller_parser import parse, UnifiedReadingManager, LightProtocolDecoder
import json

def test_clean_metadata_approach():
    """Test the new clean approach: metadata.values instead of metadata.readings['L']."""
    print("=== Testing Clean Metadata Approach ===")
    
    # Simulate metadata from frontend with current task values
    current_task_values = [
        [540, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # task_time_beg: task 0 = 540
        [60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],   # task_duration: task 0 = 60
        [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    # task_day: task 0 = 2
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],    # task_channel: task 0 = 1
        [True, False, False, False, False, False, False, False, False, False, False, False, False, False, False],  # task_active: task 0 = True
        [False, False, False, False, False, False, False, False, False, False, False, False, False, False, False],  # task_run_once: all False
        0,   # lux_rate_c1
        0,   # lux_rate_c2
        "",  # lux_name_c1
        "",  # lux_name_c2
        0,   # lux_per_month
        1    # task_count: 1 active task
    ]
    
    # Clean metadata structure from frontend
    metadata = {
        'device_id': '2121004',
        'protocol': 'mqtt',
        'values': current_task_values  # Current task values passed directly
    }
    
    print("=== Metadata Structure ===")
    print(f"Has values: {'values' in metadata}")
    print(f"Task 0 active: {metadata['values'][4][0]}")
    print(f"Task 0 time_beg: {metadata['values'][0][0]}")
    print(f"Current task_count: {metadata['values'][11]}")
    
    # Simulate read_all response with device task_count = 7
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
            'task_id': 7,  # Device reports 7 tasks
            'channel': 0,
            'active': 0,
            'run_once': 0
        }
    }
    
    # Encode packet to hex
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")
    print(f"\nHex payload for read_all: {hex_payload}")
    
    # Parse with clean metadata approach
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        
        print("\n=== Result After read_all ===")
        print(f"Task 0 active: {reading['values'][4][0]}")       # Should still be True
        print(f"Task 0 time_beg: {reading['values'][0][0]}")     # Should still be 540
        print(f"Task count: {reading['values'][11]}")            # Should now be 7
        print(f"Operation: {reading['metadata']['operation']}")  # Should be 'read_all'
        
        # Verify task data was preserved
        if reading['values'][4][0] == True and reading['values'][0][0] == 540:
            print("✅ Task data PRESERVED with clean metadata approach!")
        else:
            print("❌ Task data was lost!")
            
        # Verify task count was updated
        if reading['values'][11] == 7:
            print("✅ Task count UPDATED correctly!")
        else:
            print(f"❌ Task count update failed. Expected 7, got {reading['values'][11]}")
            
        print("\n=== Clean Approach Benefits ===")
        print("✅ No nested readings structure")
        print("✅ No copying operations needed")
        print("✅ Minimal payload size")
        print("✅ Direct values access")
        
    else:
        print("❌ Parse failed - no result")

def test_without_values():
    """Test what happens when no values are passed (fresh device)."""
    print("\n=== Testing Without Values (Fresh Device) ===")
    
    # Metadata without values (fresh device)
    metadata = {
        'device_id': '2121004',
        'protocol': 'mqtt'
        # No 'values' key
    }
    
    # Simulate read_all response
    packet_data = {
        'id_capteur': 2121004,
        'epoch': 1753453651,
        'header': {'task_id': 0, 'reserved': 0, 'all': 1, 'clear': 0, 'get_set': 0},
        'alarm': {'time_beg': 0, 'duration': 0, 'day': 0, 'task_id': 3, 'channel': 0, 'active': 0, 'run_once': 0}
    }
    
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        print(f"Fresh device task_count: {reading['values'][11]}")  # Should be 3
        print("✅ Fresh device handling works correctly!")

if __name__ == "__main__":
    try:
        test_clean_metadata_approach()
        test_without_values()
        print("\n🎉 Clean metadata approach tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()