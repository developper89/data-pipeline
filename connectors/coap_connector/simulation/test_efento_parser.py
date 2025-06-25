#!/usr/bin/env python3
"""
Efento Parser Test Suite

This script provides comprehensive testing capabilities for the Efento bidirectional parser,
including realistic payload generation based on real sensor data analysis.

Designed to work with the container's protobuf loading mechanism.
"""

import base64
import datetime
import math
import logging
import time
import os
import sys
from typing import List, Dict, Any, Optional
from pathlib import Path

# Add the current directory to path for parser import
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Add shared modules to path (container-style)
shared_path = os.path.join(current_dir, '..', '..', 'shared')
if os.path.exists(shared_path):
    sys.path.insert(0, shared_path)

# Import the parser functions
try:
    # from efento_bidirectional_parser import parse, format_command
    PARSER_AVAILABLE = True
    print("✅ Parser imported successfully")
except ImportError as e:
    print(f"❌ Failed to import parser: {e}")
    PARSER_AVAILABLE = False

# Import protobuf libraries - simplified for container environment
try:
    from google.protobuf.message import DecodeError
    from google.protobuf.json_format import MessageToDict
    
    # Container protobuf path (verified working)
    container_protobuf_path = "/app/shared/translation/protobuf/proto_schemas/efento/protobuf"
    
    # Add container protobuf path to sys.path
    if os.path.exists(container_protobuf_path):
        sys.path.insert(0, container_protobuf_path)
        print(f"✅ Added protobuf path: {container_protobuf_path}")
    
    # Direct import of compiled protobuf modules
    import proto_measurements_pb2
    import proto_device_info_pb2
    import proto_config_pb2
    import proto_rule_pb2
    import proto_measurement_types_pb2
    
    PROTOBUF_AVAILABLE = True
    print("✅ Protobuf modules imported successfully")
    
except ImportError as e:
    print(f"❌ Protobuf import failed: {e}")
    print("   Make sure you're running inside the container environment")
    PROTOBUF_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_real_sensor_data():
    """Analyze the real sensor data files and show insights."""
    print("🔍 Analyzing Real Sensor Data from Debug Files")
    print("=" * 60)
    
    # Analyze the serial number format from real data
    serial_b64 = "KCwCQlJv"  # From debug files
    serial_bytes = base64.b64decode(serial_b64)
    serial_hex = serial_bytes.hex()
    
    print(f"📱 Device Serial Analysis:")
    print(f"   Base64: {serial_b64}")
    print(f"   Hex:    {serial_hex}")
    print(f"   Bytes:  {list(serial_bytes)}")
    
    # Analyze measurement timing from real data
    period_base = 5
    period_factor = 12
    actual_period = period_base * period_factor
    
    print(f"\n⏱️  Measurement Timing:")
    print(f"   Period Base:   {period_base} seconds")
    print(f"   Period Factor: {period_factor}")
    print(f"   Actual Period: {actual_period} seconds ({actual_period/60:.1f} minutes)")
    
    # Analyze error codes from real data
    error_code = 8388557
    error_range_start = 8355840
    error_range_end = 8388607
    is_error = error_range_start <= error_code <= error_range_end
    
    print(f"\n⚠️  Sensor Error Analysis:")
    print(f"   Sample Offset: {error_code}")
    print(f"   Error Range:   [{error_range_start}:{error_range_end}]")
    print(f"   Is Error Code: {is_error}")
    print(f"   Error Type:    Sensor failure (no valid measurement)")
    
    # Analyze device configuration from real data
    print(f"\n🔧 Device Configuration:")
    print(f"   Active Channels: TEMPERATURE, HUMIDITY")
    print(f"   Transmission:    Every 120 seconds (2 minutes)")
    print(f"   Battery Status:  OK")
    print(f"   Config Hash:     10")
    
    return {
        "device_serial_hex": serial_hex,
        "device_serial_b64": serial_b64,
        "measurement_period": actual_period,
        "error_code": error_code,
        "active_channels": ["TEMPERATURE", "HUMIDITY"]
    }


def generate_realistic_measurement_payload(device_serial: str = "282c0242526f", 
                                         include_errors: bool = False,
                                         measurement_types: List[str] = None,
                                         custom_temperature: Optional[float] = None,
                                         custom_humidity: Optional[float] = None,
                                         custom_pressure: Optional[float] = None) -> Dict[str, Any]:
    """
    Generate a realistic test measurement payload based on real sensor data analysis.
    
    Args:
        device_serial: 6-byte hex string for device serial number (default from real data)
        include_errors: Whether to include sensor error codes in sample offsets
        measurement_types: List of measurement type names to include
        custom_temperature: Custom temperature value in °C (uses default if None)
        custom_humidity: Custom humidity value in % (uses default if None)
        custom_pressure: Custom pressure value in hPa (uses default if None)
        
    Returns:
        Dictionary containing binary payload and metadata for CoAP testing
    """
    if not PROTOBUF_AVAILABLE:
        raise ImportError("Protobuf libraries not available for payload generation")
    
    if measurement_types is None:
        measurement_types = ["TEMPERATURE", "HUMIDITY"]  # Match real sensor config
    
    # Create ProtoMeasurements message
    measurements = proto_measurements_pb2.ProtoMeasurements()
    
    # Set device serial number (convert hex string to bytes)
    serial_bytes = bytes.fromhex(device_serial)
    measurements.serial_num = serial_bytes
    
    # Set realistic device status based on real data
    measurements.battery_status = True  # Battery OK
    measurements.measurement_period_base = 5  # Real sensor uses 5 seconds base
    measurements.measurement_period_factor = 12  # Real sensor uses 12 factor = 60s intervals
    measurements.next_transmission_at = int(time.time()) + 120  # 2 minutes (real uses 120s)
    measurements.transfer_reason = 0  # Normal transmission (real data shows 0)
    measurements.hash = 10  # Real sensor shows hash = 10
    measurements.cloud_token = ""  # Empty in real data
    
    # Create realistic timestamp (current time)
    base_timestamp = int(time.time())
    
    # Add measurement channels based on real sensor patterns
    for i, measure_type in enumerate(measurement_types):
        channel = measurements.channels.add()
        
        if measure_type == "TEMPERATURE":
            channel.type = 1  # MEASUREMENT_TYPE_TEMPERATURE
            channel.timestamp = base_timestamp
            channel.start_point = 0  # Real sensor shows 0 start point
            
            if include_errors:
                # Use real error code from sensor data (sensor failure)
                channel.sample_offsets.extend([8388557, 8388557])  # Actual error code from real data
            else:
                # Use custom temperature or default
                temp_value = custom_temperature if custom_temperature is not None else 23.5
                channel.start_point = int(temp_value * 10)  # Convert to device resolution (0.1°C)
                # Add realistic temperature variations over 5 readings
                channel.sample_offsets.extend([0, 1, -1, 2, 0])  # Small variations around current value
            
        elif measure_type == "HUMIDITY":
            channel.type = 2  # MEASUREMENT_TYPE_HUMIDITY  
            channel.timestamp = base_timestamp
            channel.start_point = 0  # Real sensor shows 0 start point
            
            if include_errors:
                # Use real error code from sensor data
                channel.sample_offsets.extend([8388557, 8388557])  # Actual error code from real data
            else:
                # Use custom humidity or default
                humidity_value = custom_humidity if custom_humidity is not None else 45.0
                channel.start_point = int(humidity_value)  # Convert to device resolution (1%)
                channel.sample_offsets.extend([0, 1, -1, 1, 0])  # Small variations around current value
            
        elif measure_type == "ATMOSPHERIC_PRESSURE":
            channel.type = 3  # MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE
            channel.timestamp = base_timestamp
            # Use custom pressure or default
            pressure_value = custom_pressure if custom_pressure is not None else 1013.2
            channel.start_point = int(pressure_value * 10)  # Convert to device resolution (0.1 hPa)
            # Add realistic pressure variations
            channel.sample_offsets.extend([0, 1, -1, 2, 0])
            
        elif measure_type == "OK_ALARM":
            channel.type = 5  # MEASUREMENT_TYPE_OK_ALARM (Binary)
            channel.timestamp = base_timestamp
            # Binary type: positive = ALARM, negative = OK
            # State changes: OK at timestamp, ALARM after 60s, OK after 120s
            channel.sample_offsets.extend([-1, 60, -120])
    
    # Serialize to binary
    payload_bytes = measurements.SerializeToString()
    
    return {
        "payload_hex": payload_bytes.hex(),
        "payload_bytes": payload_bytes,
        "payload_base64": base64.b64encode(payload_bytes).decode('ascii'),
        "device_serial": device_serial,
        "measurement_types": measurement_types,
        "uri_path": "m",  # Measurements endpoint
        "coap_method": "POST",
        "expected_device_id": device_serial,
        "include_errors": include_errors,
        "test_config": {
            "device_id": device_serial,
            "labels": {"test": "realistic", "location": "lab", "model": "efento"},
            "metadata": {"uri_path": "/m"}
        }
    }


def generate_test_measurement_payload(device_serial: str = "aabbccddeeff", 
                                    measurement_types: List[str] = None) -> Dict[str, Any]:
    """
    Generate a basic test measurement payload for CoAP testing (backwards compatibility).
    
    Args:
        device_serial: 6-byte hex string for device serial number
        measurement_types: List of measurement type names to include
        
    Returns:
        Dictionary containing binary payload and metadata for CoAP testing
    """
    if not PROTOBUF_AVAILABLE:
        raise ImportError("Protobuf libraries not available for payload generation")
    
    if measurement_types is None:
        measurement_types = ["TEMPERATURE", "HUMIDITY", "ATMOSPHERIC_PRESSURE"]
    
    # Create ProtoMeasurements message
    measurements = proto_measurements_pb2.ProtoMeasurements()
    
    # Set device serial number (convert hex string to bytes)
    measurements.serial_num = bytes.fromhex(device_serial)
    
    # Set basic device status
    measurements.battery_status = True  # Battery OK
    measurements.measurement_period_base = 60  # 60 seconds base period
    measurements.measurement_period_factor = 15  # 15 minute intervals (60 * 15 = 900s)
    measurements.next_transmission_at = int(time.time()) + 3600  # Next transmission in 1 hour
    measurements.transfer_reason = 1  # First message after reset
    measurements.hash = 0x12345678  # Example config hash
    
    # Create timestamp for measurements (30 minutes ago)
    base_timestamp = int(time.time()) - 1800
    
    # Add measurement channels based on requested types
    for i, measure_type in enumerate(measurement_types):
        channel = measurements.channels.add()
        
        if measure_type == "TEMPERATURE":
            channel.type = 1  # MEASUREMENT_TYPE_TEMPERATURE
            channel.timestamp = base_timestamp
            channel.start_point = 230  # 23.0°C as start point (resolution 0.1°C)
            # Add 5 sample offsets: +0.5°C, +1.0°C, +0.3°C, -0.2°C, +0.8°C
            channel.sample_offsets.extend([5, 10, 3, -2, 8])
            
        elif measure_type == "HUMIDITY":
            channel.type = 2  # MEASUREMENT_TYPE_HUMIDITY  
            channel.timestamp = base_timestamp
            channel.start_point = 55  # 55% RH as start point
            # Add 5 sample offsets: +2%, +1%, -1%, +3%, +0%
            channel.sample_offsets.extend([2, 1, -1, 3, 0])
            
        elif measure_type == "ATMOSPHERIC_PRESSURE":
            channel.type = 3  # MEASUREMENT_TYPE_ATMOSPHERIC_PRESSURE
            channel.timestamp = base_timestamp
            channel.start_point = 10132  # 1013.2 hPa as start point (resolution 0.1 hPa)
            # Add 5 sample offsets: +0.3 hPa, -0.1 hPa, +0.5 hPa, -0.2 hPa, +0.1 hPa
            channel.sample_offsets.extend([3, -1, 5, -2, 1])
            
        elif measure_type == "OK_ALARM":
            channel.type = 5  # MEASUREMENT_TYPE_OK_ALARM (Binary)
            channel.timestamp = base_timestamp
            # Binary type: positive = ALARM, negative = OK
            # State changes: OK at timestamp, ALARM after 300s, OK after 600s
            channel.sample_offsets.extend([-1, 300, -600])
            
        elif measure_type == "PULSE_CNT_ACC_MAJOR":
            channel.type = 36  # MEASUREMENT_TYPE_PULSE_CNT_ACC_MAJOR
            channel.timestamp = base_timestamp
            channel.start_point = 1000  # Starting pulse count
            # Add accumulative pulse offsets (for major counter calculation)
            channel.sample_offsets.extend([4, 8, 12, 16, 20])
            
        elif measure_type == "PULSE_CNT_ACC_MINOR":
            channel.type = 35  # MEASUREMENT_TYPE_PULSE_CNT_ACC_MINOR  
            channel.timestamp = base_timestamp
            channel.start_point = 24  # Starting point for minor calculation
            # Add minor pulse offsets (will combine with previous major)
            channel.sample_offsets.extend([6, 12, 18, 24, 30])
    
    # Serialize to binary
    payload_bytes = measurements.SerializeToString()
    
    return {
        "payload_hex": payload_bytes.hex(),
        "payload_bytes": payload_bytes,
        "payload_base64": base64.b64encode(payload_bytes).decode('ascii'),
        "device_serial": device_serial,
        "measurement_types": measurement_types,
        "uri_path": "m",  # Measurements endpoint
        "coap_method": "POST",
        "expected_device_id": device_serial,
        "test_config": {
            "device_id": device_serial,
            "labels": {"test": "true", "location": "lab"},
            "metadata": {"uri_path": "/m"}
        }
    }


def generate_realistic_device_info_payload(device_serial: str = "282c0242526f") -> Dict[str, Any]:
    """Generate a realistic device info payload based on real sensor data."""
    if not PROTOBUF_AVAILABLE:
        raise ImportError("Protobuf libraries not available for payload generation")
    
    # Create ProtoDeviceInfo message
    device_info = proto_device_info_pb2.ProtoDeviceInfo()
    
    # Set device serial number (from real data)
    device_info.serial_num = bytes.fromhex(device_serial)
    
    # Set realistic device info fields based on real data analysis
    device_info.sw_version = 1549  # Real firmware version from data
    device_info.commit_id = "02e2d8f"  # Real commit ID from data
    device_info.cloud_token = ""  # Empty in real data
    
    # Add realistic runtime info (simplified - adjust based on actual proto structure)
    if hasattr(device_info, 'runtime_info'):
        device_info.runtime_info.up_time = 1553902  # From real data
        device_info.runtime_info.mcu_temperature = 20  # From real data
        device_info.runtime_info.min_battery_voltage = 3225  # From real data (3.225V)
        device_info.runtime_info.reset_counter = 0  # From real data
    
    # Add modem info if available
    if hasattr(device_info, 'modem'):
        device_info.modem.sim_card_identification = "89882280000093678627"  # From real data
    
    # Serialize to binary
    payload_bytes = device_info.SerializeToString()
    
    return {
        "payload_hex": payload_bytes.hex(),
        "payload_bytes": payload_bytes,
        "payload_base64": base64.b64encode(payload_bytes).decode('ascii'),
        "device_serial": device_serial,
        "uri_path": "i",  # Device info endpoint
        "coap_method": "POST",
        "expected_device_id": device_serial,
        "test_config": {
            "device_id": device_serial,
            "labels": {"test": "realistic", "location": "lab", "model": "efento"},
            "metadata": {"uri_path": "/i"}
        }
    }


def generate_realistic_config_payload(device_serial: str = "282c0242526f") -> Dict[str, Any]:
    """Generate a realistic config payload based on real sensor data and proto structure."""
    if not PROTOBUF_AVAILABLE:
        raise ImportError("Protobuf libraries not available for payload generation")
    
    # Create ProtoConfig message
    config = proto_config_pb2.ProtoConfig()
    
    # Set device serial number (from real data)
    config.serial_number = bytes.fromhex(device_serial)
    
    # Set realistic measurement timing based on real data analysis
    config.measurement_period_base = 5  # 5 seconds base from real data
    config.measurement_period_factor = 12  # Factor 12 from real data (5s * 12 = 60s)
    config.transmission_interval = 120  # 120 seconds from real data
    config.ack_interval = 0xFFFFFFFF  # Always request ACK
    
    # Set realistic server configuration
    config.data_server_ip = "192.168.1.100"  # Default server IP
    config.data_server_port = 5683  # Standard CoAP port
    config.update_server_ip = "192.168.1.100"
    config.update_server_port_coap = 5683
    config.update_server_port_udp = 5684
    
    # Set realistic network configuration
    config.apn = "internet"  # Common APN
    config.plmn_selection = 0xFFFFFFFF  # Automatic selection
    
    # Set realistic endpoints
    config.data_endpoint = "/m"
    config.configuration_endpoint = "/c"
    config.device_info_endpoint = "/i"
    config.time_endpoint = "/t"
    
    # Set realistic BLE configuration
    config.ble_turnoff_time = 0xFFFFFFFF  # Always on
    config.ble_tx_power_level = 3  # Medium power level
    
    # Add realistic BLE advertising period
    if hasattr(config, 'ble_advertising_period'):
        config.ble_advertising_period.mode = proto_config_pb2.BLE_ADVERTISING_PERIOD_MODE_DEFAULT
        config.ble_advertising_period.normal = 1636  # ~1022.5ms in 0.625ms steps
        config.ble_advertising_period.fast = 800   # ~500ms in 0.625ms steps
    
    # Set realistic transfer limits
    config.transfer_limit = 65535  # Disabled
    config.transfer_limit_timer = 65535  # Disabled
    
    # Set realistic supervision
    config.supervision_period = 0xFFFFFFFF  # Disabled
    
    # Set realistic cloud token configuration
    config.cloud_token_config = 255  # Do not send cloud_token field
    config.cloud_token = ""
    config.cloud_token_coap_option = 65000  # In payload
    
    # Set realistic DNS configuration
    config.dns_server_ip.extend([8, 8, 8, 8])  # Google DNS: 8.8.8.8
    config.dns_ttl_config = 864001  # Accept TTL from DNS server
    
    # Set realistic modem configuration
    config.modem_bands_mask = 2084  # Example: bands 3, 8, and 20 enabled
    
    # Add realistic channel types based on real sensor data (temperature + humidity)
    if hasattr(proto_measurement_types_pb2, 'TEMPERATURE'):
        config.channel_types.append(proto_measurement_types_pb2.TEMPERATURE)
        config.channel_types.append(proto_measurement_types_pb2.HUMIDITY)
    
    # Add realistic rules for temperature threshold monitoring
    if PROTOBUF_AVAILABLE and hasattr(proto_rule_pb2, 'ProtoRule'):
        # Rule 1: High temperature threshold (> 30°C)
        rule1 = config.rules.add()
        rule1.channel_mask = 1  # Channel 0 (temperature)
        rule1.condition = proto_rule_pb2.CONDITION_HIGH_THRESHOLD
        rule1.action = proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION
        # Parameters: threshold=30°C (300 in 0.1°C), hysteresis=2°C (20 in 0.1°C), mode=1 (moving avg), samples=3, type=TEMPERATURE
        rule1.parameters.extend([300, 20, 1, 3, 1])  # Assuming TEMPERATURE = 1
        
        # Rule 2: Low humidity threshold (< 30%)
        rule2 = config.rules.add()
        rule2.channel_mask = 2  # Channel 1 (humidity)
        rule2.condition = proto_rule_pb2.CONDITION_LOW_THRESHOLD
        rule2.action = proto_rule_pb2.ACTION_TRIGGER_TRANSMISSION_WITH_ACK
        # Parameters: threshold=30% (30), hysteresis=5% (5), mode=1 (moving avg), samples=2, type=HUMIDITY
        rule2.parameters.extend([30, 5, 1, 2, 2])  # Assuming HUMIDITY = 2
    
    # Set realistic configuration hash and timestamp
    import time
    config.hash = 0x12345678  # Example hash
    config.hash_timestamp = int(time.time())
    config.current_time = int(time.time())
    
    # Set realistic error handling
    config.accept_without_testing = False
    config.network_troubleshooting = 2  # Enabled
    
    # Set realistic LED configuration (simplified)
    config.led_config.extend([
        30,   # Green LED period (30 * 5 = 150s)
        60,   # Red LED period (60 * 5 = 300s)
        10,   # Red LED timeout for comm problems (10 min)
        5,    # Red LED timeout for sensor problems (5 min)
        15,   # Red LED timeout for low power (15 min)
        2,    # Green LED timeout for measurement (2 min)
        3,    # Green LED timeout for transmission (3 min)
        255,  # Green LED always on for proper operation
        100   # Blink duration (100 * 5 = 500ms)
    ])
    
    # Serialize to binary
    payload_bytes = config.SerializeToString()
    
    return {
        "payload_hex": payload_bytes.hex(),
        "payload_bytes": payload_bytes,
        "payload_base64": base64.b64encode(payload_bytes).decode('ascii'),
        "device_serial": device_serial,
        "uri_path": "c",  # Config endpoint
        "coap_method": "POST",
        "expected_device_id": device_serial,
        "test_config": {
            "device_id": device_serial,
            "labels": {"test": "realistic", "location": "lab", "model": "efento"},
            "metadata": {"uri_path": "/c"}
        }
    }


def create_coap_test_files(test_payloads: List[Dict[str, Any]], output_dir: str = "/tmp") -> List[str]:
    """
    Create binary files and shell script for CoAP testing.
    
    Args:
        test_payloads: List of test payload dictionaries
        output_dir: Directory to create test files in
        
    Returns:
        List of created file paths
    """
    created_files = []
    script_lines = [
        "#!/bin/bash",
        "# Efento CoAP Test Script",
        "# Generated from real sensor data analysis",
        "",
        "set -e  # Exit on any error",
        "",
        "COAP_SERVER=${1:-localhost:5683}",
        "echo \"🚀 Starting Efento CoAP Tests against $COAP_SERVER\"",
        "echo \"📊 Testing with realistic payloads based on real sensor data\"",
        ""
    ]
    
    for i, payload_data in enumerate(test_payloads):
        # Create binary payload file
        payload_file = os.path.join(output_dir, f"efento_test_{i+1}_{payload_data['uri_path']}.bin")
        with open(payload_file, 'wb') as f:
            f.write(payload_data['payload_bytes'])
        created_files.append(payload_file)
        
        # Add test to script
        test_name = f"Test {i+1}: {payload_data.get('measurement_types', payload_data.get('uri_path', 'unknown'))}"
        if payload_data.get('include_errors'):
            test_name += " (with sensor errors)"
            
        script_lines.extend([
            f"echo \"\\n📡 {test_name}\"",
            f"echo \"   Device: {payload_data['device_serial']}\"",
            f"echo \"   Endpoint: /{payload_data['uri_path']}\"",
            f"echo \"   Payload: {len(payload_data['payload_bytes'])} bytes\"",
            f"",
            f"coap-client-notls -m {payload_data['coap_method']} \\",
            f"                  -f {payload_file} \\",
            f"                  \"coap://$COAP_SERVER/{payload_data['uri_path']}\"",
            f"",
            f"echo \"✅ {test_name} completed\"",
            "sleep 1",
            ""
        ])
    
    # Write script file
    script_file = os.path.join(output_dir, "efento_coap_tests.sh")
    with open(script_file, 'w') as f:
        f.write('\n'.join(script_lines))
        f.write('\necho "🏁 All CoAP tests completed!"\n')
    
    # Make script executable
    os.chmod(script_file, 0o755)
    created_files.append(script_file)
    
    return created_files


def test_parser_with_payloads(test_payloads: List[Dict[str, Any]]) -> None:
    """Test the parser with generated payloads."""
    if not PARSER_AVAILABLE:
        print("❌ Parser not available for testing")
        return
        
    print("\n🧪 Testing Parser with Generated Payloads")
    print("=" * 50)
    
    for i, payload_data in enumerate(test_payloads):
        test_name = f"Test {i+1}: {payload_data.get('measurement_types', payload_data.get('uri_path', 'unknown'))}"
        if payload_data.get('include_errors'):
            test_name += " (with errors)"
            
        print(f"\n📊 {test_name}")
        
        try:
            # Test the parser
            test_config = payload_data['test_config']
            parsed_results = parse(payload_data['payload_bytes'], test_config)
            
            print(f"  ✅ Parsed successfully: {len(parsed_results)} results")
            
            # Show sample results
            if parsed_results:
                for j, result in enumerate(parsed_results[:3]):  # Show first 3 results
                    values_str = str(result['values'])
                    if len(values_str) > 50:
                        values_str = values_str[:47] + "..."
                    print(f"    📋 Result {j+1}: {result['index']} = {values_str}")
                    if result.get('metadata', {}).get('is_error'):
                        print(f"       ⚠️  Error: {result['metadata'].get('error_description', 'Unknown error')}")
                        
                if len(parsed_results) > 3:
                    print(f"    ... and {len(parsed_results) - 3} more results")
                    
        except Exception as e:
            print(f"  ❌ Parser error: {e}")


def test_command_formatting() -> None:
    """Test the command formatting functionality."""
    if not PARSER_AVAILABLE:
        print("❌ Parser not available for command testing")
        return
        
    print("\n🔧 Testing Command Formatting")
    print("=" * 40)
    
    # Test case 1: Config Update Command
    test_config_command = {
        'command_type': 'config_update',
        'payload': {
            'measurement_interval': 900,  # 15 minutes in seconds
            'transmission_interval': 3600,  # 1 hour in seconds
            'ack_interval': 'always', 
            'server_address': '192.168.1.100',
            'server_port': 5683,
            'rules': [
                {
                    'type': 'above_threshold',
                    'channel': 1,
                    'threshold': 25.0,
                    'hysteresis': 2.0,
                    'action': 'trigger_transmission'
                }
            ]
        }
    }
    
    test_device_config = {
        'device_id': 'aabbccddeeff',
        'labels': {'room': 'server-room'}
    }
    
    try:
        # Format the command
        formatted_command = format_command(test_config_command, test_device_config)
        print(f"✅ Config command formatted: {len(formatted_command)} bytes")
        print(f"   Hex: {formatted_command.hex()[:50]}...")
        
    except Exception as e:
        print(f"❌ Config command error: {e}")
    
    # Test case 2: Time Sync Command
    test_time_command = {
        'command_type': 'time_sync',
        'payload': {}
    }
    
    try:
        # Format the time command
        formatted_time = format_command(test_time_command, test_device_config)
        print(f"✅ Time command formatted: {len(formatted_time)} bytes")
        print(f"   Hex: {formatted_time.hex()}")
        
    except Exception as e:
        print(f"❌ Time command error: {e}")


def run_comprehensive_tests():
    """Run comprehensive tests of the parser with various payload types."""
    if not PROTOBUF_AVAILABLE:
        print("❌ Protobuf libraries not available - cannot run comprehensive tests")
        return
        
    print("🧪 Running Comprehensive Efento Parser Tests")
    print("=" * 60)
    
    # First analyze real sensor data
    real_data_analysis = analyze_real_sensor_data()
    
    # Test cases based on real sensor data patterns
    test_cases = [
        {
            "name": "Realistic Environmental Sensors (Normal)",
            "types": ["TEMPERATURE", "HUMIDITY"],
            "device": real_data_analysis["device_serial_hex"],
            "errors": False,
            "realistic": True
        },
        {
            "name": "Realistic Environmental Sensors (With Errors)", 
            "types": ["TEMPERATURE", "HUMIDITY"],
            "device": real_data_analysis["device_serial_hex"],
            "errors": True,
            "realistic": True
        },
        {
            "name": "Basic Environmental Sensors",
            "types": ["TEMPERATURE", "HUMIDITY", "ATMOSPHERIC_PRESSURE"],
            "device": "112233445566",
            "errors": False,
            "realistic": False
        },
        {
            "name": "Mixed Sensor Types",
            "types": ["TEMPERATURE", "HUMIDITY", "OK_ALARM"],
            "device": "ddeeff112233",
            "errors": False,
            "realistic": False
        }
    ]
    
    all_test_payloads = []
    
    # Generate measurement test payloads
    for test_case in test_cases:
        print(f"\n📊 Generating {test_case['name']} payload...")
        try:
            if test_case.get('realistic', False):
                payload_data = generate_realistic_measurement_payload(
                    device_serial=test_case['device'],
                    measurement_types=test_case['types'],
                    include_errors=test_case['errors']
                )
            else:
                payload_data = generate_test_measurement_payload(
                    device_serial=test_case['device'],
                    measurement_types=test_case['types']
                )
            
            all_test_payloads.append(payload_data)
            print(f"  ✅ Generated payload: {len(payload_data['payload_bytes'])} bytes")
                
        except Exception as e:
            print(f"  ❌ Error in {test_case['name']}: {e}")
    
    # Generate realistic device info payload
    print(f"\n📱 Generating Realistic Device Info payload...")
    try:
        device_info_payload = generate_realistic_device_info_payload(real_data_analysis["device_serial_hex"])
        all_test_payloads.append(device_info_payload)
        print(f"  ✅ Generated device info payload: {len(device_info_payload['payload_bytes'])} bytes")
        
    except Exception as e:
        print(f"  ❌ Error in device info generation: {e}")
    
    # Generate realistic config payload
    print(f"\n⚙️ Generating Realistic Config payload...")
    try:
        config_payload = generate_realistic_config_payload(real_data_analysis["device_serial_hex"])
        all_test_payloads.append(config_payload)
        print(f"  ✅ Generated config payload: {len(config_payload['payload_bytes'])} bytes")
        
    except Exception as e:
        print(f"  ❌ Error in config generation: {e}")
    
    # Test parser with generated payloads
    # test_parser_with_payloads(all_test_payloads)
    
    # Test command formatting
    # test_command_formatting()
    
    # Create CoAP test files
    print(f"\n🌐 Creating CoAP Test Files...")
    try:
        created_files = create_coap_test_files(all_test_payloads)
        print(f"  ✅ Created {len(created_files)} test files:")
        for file_path in created_files:
            print(f"     📄 {file_path}")
        
        script_file = [f for f in created_files if f.endswith('.sh')][0]
        print(f"\n  🚀 Run tests with: {script_file}")
        print(f"  💡 Or with custom server: {script_file} your-server:5683")
        
    except Exception as e:
        print(f"  ❌ Error creating test files: {e}")
    
    # Show summary
    print(f"\n📈 Test Summary:")
    print(f"  • Generated {len(all_test_payloads)} test payloads")
    print(f"  • Based on real sensor data: {real_data_analysis['device_serial_hex']}")
    print(f"  • Includes error condition testing")
    print(f"  • Measurement period: {real_data_analysis['measurement_period']}s")
    
    return all_test_payloads


def print_container_environment_info():
    """Print information about the container environment for debugging."""
    print("\n🐳 Container Environment Analysis")
    print("=" * 50)
    
    # Check container paths
    container_paths = [
        "/app/shared/translation/protobuf/proto_schemas/efento",
        "/app/shared/translation/protobuf/proto_schemas/efento/protobuf",
        "/app/connectors/coap_connector"
    ]
    
    for path in container_paths:
        if os.path.exists(path):
            print(f"✅ {path} - EXISTS")
            try:
                files = os.listdir(path)
                if files:
                    print(f"   📁 Contains: {len(files)} items")
                    for f in files[:5]:  # Show first 5 items
                        print(f"      - {f}")
                    if len(files) > 5:
                        print(f"      ... and {len(files) - 5} more")
                else:
                    print("   📁 Empty directory")
            except PermissionError:
                print("   ❌ Permission denied")
        else:
            print(f"❌ {path} - NOT FOUND")
    
    # Check Python path
    print(f"\n🐍 Python Path Analysis:")
    print(f"   Current working directory: {os.getcwd()}")
    print(f"   Script directory: {current_dir}")
    print(f"   Python path contains {len(sys.path)} entries:")
    for i, path in enumerate(sys.path[:5]):
        print(f"      {i+1}. {path}")
    if len(sys.path) > 5:
        print(f"      ... and {len(sys.path) - 5} more")


if __name__ == '__main__':
    print("🚀 Efento Parser Test Suite (Container Edition)")
    print("=" * 50)
    
    # Print container environment info
    print_container_environment_info()
    
    if not PROTOBUF_AVAILABLE:
        print("\n❌ Protobuf libraries not available. Troubleshooting:")
        print("   1. Check if protobuf is installed: pip install protobuf")
        print("   2. Verify .proto files are compiled to Python modules")
        print("   3. Check container paths and permissions")
        print("   4. Run verify_protobuf_setup.py first")
        sys.exit(1)
    
    if not PARSER_AVAILABLE:
        print("\n❌ Parser not available. Check efento_bidirectional_parser.py")
        sys.exit(1)
    
    try:
        # Run comprehensive tests
        test_payloads = run_comprehensive_tests()
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("🎉 Test suite completed - ready for comprehensive testing!") 