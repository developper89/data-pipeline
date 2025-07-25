#!/usr/bin/env python3
"""
Test script for the unified reading implementation in light_controller_parser.py
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from light_controller_parser import parse, UnifiedReadingManager, LightProtocolDecoder
import json

def test_read_all_operation():
    """Test read_all operation - should update task_count."""
    print("=== Testing READ_ALL Operation ===")
    
    # Simulate a read_all response with 3 tasks configured
    metadata = {'device_id': '2121004'}
    
    # Create a packet for read_all operation (get_set=0, all=1, clear=0)
    # with task_count=3 in alarm.task_id
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
            'task_id': 3,  # task_count = 3
            'channel': 0,
            'active': 0,
            'run_once': 0
        }
    }
    
    # Encode packet to hex
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")
    print(f"Hex payload: {hex_payload}")
    
    # Parse the payload
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        print(f"Task count updated to: {reading['values'][11]}")  # task_count is at index 11
        print(f"Metadata readings stored: {'L' in metadata.get('readings', {})}")
        assert reading['values'][11] == 3, f"Expected task_count 3, got {reading['values'][11]}"
        print("✅ READ_ALL test passed")
    else:
        print("❌ READ_ALL test failed - no result")
    
    return metadata

def test_create_task_operation(metadata):
    """Test create_task operation - should update specific task data."""
    print("\n=== Testing CREATE_TASK Operation ===")
    
    # Create a packet for create_task operation (get_set=1, all=0, clear=0)
    # Task ID 0: Start at 9:00 (540 min), Duration 8h (480 min), Monday (1), Channel 0, Active
    packet_data = {
        'id_capteur': 2121004,
        'epoch': 1753453651,
        'header': {
            'task_id': 0,
            'reserved': 0,
            'all': 0,
            'clear': 0,
            'get_set': 1   # write operation
        },
        'alarm': {
            'time_beg': 540,   # 9:00 AM
            'duration': 480,   # 8 hours
            'day': 1,          # Monday
            'task_id': 0,      # Task ID 0
            'channel': 0,      # Channel 0
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
        print(f"Task 0 time_beg: {reading['values'][0][0]}")      # Should be 540
        print(f"Task 0 duration: {reading['values'][1][0]}")     # Should be 480
        print(f"Task 0 day: {reading['values'][2][0]}")          # Should be 1
        print(f"Task 0 active: {reading['values'][4][0]}")       # Should be True
        print(f"Updated task_count: {reading['values'][11]}")     # Should be 1 (counting active tasks)
        
        assert reading['values'][0][0] == 540, f"Expected time_beg 540, got {reading['values'][0][0]}"
        assert reading['values'][1][0] == 480, f"Expected duration 480, got {reading['values'][1][0]}"
        assert reading['values'][2][0] == 1, f"Expected day 1, got {reading['values'][2][0]}"
        assert reading['values'][4][0] == True, f"Expected active True, got {reading['values'][4][0]}"
        print("✅ CREATE_TASK test passed")
    else:
        print("❌ CREATE_TASK test failed - no result")
    
    return metadata

def test_delete_task_operation(metadata):
    """Test delete_task operation - should reset specific task data."""
    print("\n=== Testing DELETE_TASK Operation ===")
    
    # Create a packet for delete_task operation (get_set=1, all=0, clear=1)
    packet_data = {
        'id_capteur': 2121004,
        'epoch': 1753453651,
        'header': {
            'task_id': 0,
            'reserved': 0,
            'all': 0,
            'clear': 1,    # delete
            'get_set': 1   # write operation
        },
        'alarm': {
            'time_beg': 0,
            'duration': 0,
            'day': 0,
            'task_id': 0,  # Delete task ID 0
            'channel': 0,
            'active': 0,
            'run_once': 0
        }
    }
    
    # Encode packet to hex
    hex_payload = LightProtocolDecoder.encode_packet(packet_data, format="hex")
    print(f"Hex payload: {hex_payload}")
    
    # Parse the payload
    result = parse(hex_payload, metadata, {})
    
    if result:
        reading = result.readings[0]
        print(f"Task 0 after delete - time_beg: {reading['values'][0][0]}")  # Should be 0
        print(f"Task 0 after delete - active: {reading['values'][4][0]}")    # Should be False
        print(f"Updated task_count: {reading['values'][11]}")                # Should be 0
        
        assert reading['values'][0][0] == 0, f"Expected time_beg 0, got {reading['values'][0][0]}"
        assert reading['values'][4][0] == False, f"Expected active False, got {reading['values'][4][0]}"
        assert reading['values'][11] == 0, f"Expected task_count 0, got {reading['values'][11]}"
        print("✅ DELETE_TASK test passed")
    else:
        print("❌ DELETE_TASK test failed - no result")

def test_unified_reading_structure():
    """Test the structure of the unified reading matches datatype schema."""
    print("\n=== Testing Unified Reading Structure ===")
    
    metadata = {'device_id': '2121004'}
    
    # Create base reading
    reading = UnifiedReadingManager.create_base_reading('2121004', metadata)
    
    print(f"Number of values: {len(reading['values'])}")
    print(f"Number of labels: {len(reading['labels'])}")
    print(f"Number of display_names: {len(reading['display_names'])}")
    
    # Check structure
    assert len(reading['values']) == 12, f"Expected 12 values, got {len(reading['values'])}"
    assert len(reading['labels']) == 12, f"Expected 12 labels, got {len(reading['labels'])}"
    assert len(reading['display_names']) == 12, f"Expected 12 display_names, got {len(reading['display_names'])}"
    
    # Check task lists have 15 elements
    for i in range(6):  # First 6 are task lists
        assert len(reading['values'][i]) == 15, f"Expected 15 elements in values[{i}], got {len(reading['values'][i])}"
    
    # Check single values
    assert isinstance(reading['values'][6], int), f"Expected int for lux_rate_c1, got {type(reading['values'][6])}"
    assert isinstance(reading['values'][7], int), f"Expected int for lux_rate_c2, got {type(reading['values'][7])}"
    assert isinstance(reading['values'][8], str), f"Expected str for lux_name_c1, got {type(reading['values'][8])}"
    assert isinstance(reading['values'][9], str), f"Expected str for lux_name_c2, got {type(reading['values'][9])}"
    assert isinstance(reading['values'][10], int), f"Expected int for lux_per_month, got {type(reading['values'][10])}"
    assert isinstance(reading['values'][11], int), f"Expected int for task_count, got {type(reading['values'][11])}"
    
    print("✅ Unified reading structure test passed")
    
    # Print sample structure for verification
    print("\nSample reading structure:")
    print(json.dumps({
        "values_structure": [
            f"task_time_beg: {len(reading['values'][0])} elements",
            f"task_duration: {len(reading['values'][1])} elements", 
            f"task_day: {len(reading['values'][2])} elements",
            f"task_channel: {len(reading['values'][3])} elements",
            f"task_active: {len(reading['values'][4])} elements",
            f"task_run_once: {len(reading['values'][5])} elements",
            f"lux_rate_c1: {type(reading['values'][6]).__name__}",
            f"lux_rate_c2: {type(reading['values'][7]).__name__}",
            f"lux_name_c1: {type(reading['values'][8]).__name__}",
            f"lux_name_c2: {type(reading['values'][9]).__name__}",
            f"lux_per_month: {type(reading['values'][10]).__name__}",
            f"task_count: {type(reading['values'][11]).__name__}",
        ],
        "labels": reading['labels'],
        "datatype_id": reading['metadata']['datatype_id']
    }, indent=2))

def main():
    """Run all tests."""
    print("Starting unified reading implementation tests...")
    
    try:
        # Test structure
        test_unified_reading_structure()
        
        # Test operations in sequence
        metadata = test_read_all_operation()
        metadata = test_create_task_operation(metadata)
        test_delete_task_operation(metadata)
        
        print("\n🎉 All tests passed! Unified reading implementation is working correctly.")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())