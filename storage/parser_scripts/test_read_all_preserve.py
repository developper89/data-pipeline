#!/usr/bin/env python3
"""
Test script to reproduce the read_all issue where existing task data gets erased.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from light_controller_parser import parse, UnifiedReadingManager, LightProtocolDecoder
import json

def test_read_all_preserves_existing_tasks():
    """Test that read_all preserves existing task data and only updates count."""
    print("=== Testing READ_ALL Preserving Existing Tasks ===")
    
    # Start with metadata that has existing task data (like after read_task)
    metadata = {
        'device_id': '2121004',
        'readings': {
            'L': {
                'device_id': '2121004',
                'values': [
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
                ],
                'labels': UnifiedReadingManager.DATATYPE_LABELS,
                'display_names': UnifiedReadingManager.DATATYPE_DISPLAY_NAMES,
                'index': 'L',
                'metadata': {
                    'operation': 'read_task',
                    'task_count': 1,
                    'device_id': '2121004'
                },
                'timestamp': '2025-07-25T18:00:47.135605'
            }
        }
    }
    
    print("=== BEFORE read_all ===")
    print(f"Task 0 active: {metadata['readings']['L']['values'][4][0]}")
    print(f"Task 0 time_beg: {metadata['readings']['L']['values'][0][0]}")
    print(f"Task count: {metadata['readings']['L']['values'][11]}")
    
    # Simulate read_all response with task_count = 7 (device says it has 7 total tasks)
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
            'task_id': 7,  # task_count = 7 (from device)
            'channel': 0,
            'active': 0,
            'run_once': 0
        }
    }
    
    # Encode packet to hex
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")
    print(f"\nHex payload for read_all: {hex_payload}")
    
    # Parse the payload - this should preserve existing task data
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        
        print("\n=== AFTER read_all ===")
        print(f"Task 0 active: {reading['values'][4][0]}")       # Should still be True
        print(f"Task 0 time_beg: {reading['values'][0][0]}")     # Should still be 540
        print(f"Task count: {reading['values'][11]}")            # Should now be 7
        print(f"Metadata task_count: {reading['metadata']['task_count']}")  # Should be 7
        
        # Verify the task data was preserved
        if reading['values'][4][0] == True and reading['values'][0][0] == 540:
            print("✅ Task data PRESERVED correctly!")
        else:
            print("❌ Task data was ERASED incorrectly!")
            print("Expected: task_active[0]=True, task_time_beg[0]=540")
            print(f"Got: task_active[0]={reading['values'][4][0]}, task_time_beg[0]={reading['values'][0][0]}")
        
        # Verify the task count was updated
        if reading['values'][11] == 7:
            print("✅ Task count UPDATED correctly!")
        else:
            print(f"❌ Task count update failed. Expected 7, got {reading['values'][11]}")
        
        # Show the result structure
        print("\n=== Result Structure ===")
        print(json.dumps({
            "task_count_updated": reading['values'][11],
            "task_0_preserved": {
                "time_beg": reading['values'][0][0],
                "duration": reading['values'][1][0], 
                "day": reading['values'][2][0],
                "channel": reading['values'][3][0],
                "active": reading['values'][4][0],
                "run_once": reading['values'][5][0]
            },
            "operation": reading['metadata']['operation']
        }, indent=2))
        
    else:
        print("❌ READ_ALL test failed - no result")
    
    return metadata

if __name__ == "__main__":
    try:
        test_read_all_preserves_existing_tasks()
        print("\n🎯 Read all preserve test completed!")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()