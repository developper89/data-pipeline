#!/usr/bin/env python3
"""
Test script specifically for read_task operation task_count calculation.
This reproduces the issue where task_count shows 0 despite having active tasks.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from light_controller_parser import parse, UnifiedReadingManager, LightProtocolDecoder
import json

def test_read_task_with_active_task():
    """Test read_task operation with an existing active task."""
    print("=== Testing READ_TASK with Active Task ==")
    
    # Start with existing metadata that has an active task but incorrect task_count=0
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
                    0    # task_count: INCORRECTLY 0 despite having 1 active task
                ],
                'labels': UnifiedReadingManager.DATATYPE_LABELS,
                'display_names': UnifiedReadingManager.DATATYPE_DISPLAY_NAMES,
                'index': 'L',
                'metadata': {
                    'operation': 'create_task',
                    'task_count': 0,  # This is wrong - should be 1
                    'device_id': '2121004'
                },
                'timestamp': '2025-07-25T17:45:01.027705'
            }
        }
    }
    
    # Simulate a read_task operation for task 0
    # This would happen when user clicks "Charger les détails" button
    packet_data = {
        'id_capteur': 2121004,
        'epoch': 1753453651,
        'header': {
            'task_id': 0,
            'reserved': 0,
            'all': 0,
            'clear': 0,
            'get_set': 0   # read operation
        },
        'alarm': {
            'time_beg': 540,   # 9:00 AM
            'duration': 60,    # 1 hour  
            'day': 2,          # Tuesday
            'task_id': 0,      # Task ID 0
            'channel': 1,      # Channel 1
            'active': 1,       # Active
            'run_once': 0      # Recurring
        }
    }
    
    # Encode packet to hex
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")
    print(f"Hex payload: {hex_payload}")
    
    # Parse the payload
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        print(f"Task 0 active status: {reading['values'][4][0]}")    # Should be True
        print(f"Task count after read_task: {reading['values'][11]}")  # Should now be 1 (corrected)
        print(f"Metadata task_count: {reading['metadata']['task_count']}")  # Should be 1
        
        # Verify the fix worked
        assert reading['values'][4][0] == True, f"Expected task 0 to be active"
        assert reading['values'][11] == 1, f"Expected task_count 1, got {reading['values'][11]}"
        assert reading['metadata']['task_count'] == 1, f"Expected metadata task_count 1, got {reading['metadata']['task_count']}"
        
        print("✅ READ_TASK task_count fix verified!")
        
        # Show the corrected reading structure
        print("\n=== Corrected Reading Structure ===")
        print(json.dumps({
            "task_count_corrected": reading['values'][11],
            "active_tasks": [i for i, active in enumerate(reading['values'][4]) if active],
            "task_0_details": {
                "time_beg": reading['values'][0][0],
                "duration": reading['values'][1][0], 
                "day": reading['values'][2][0],
                "channel": reading['values'][3][0],
                "active": reading['values'][4][0],
                "run_once": reading['values'][5][0]
            }
        }, indent=2))
        
    else:
        print("❌ READ_TASK test failed - no result")
    
    return metadata

if __name__ == "__main__":
    try:
        test_read_task_with_active_task()
        print("\n🎉 Read task count fix test passed!")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()