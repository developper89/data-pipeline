#!/usr/bin/env python3
"""
Test script to verify sensor value variations in simulate_efento_sensor.py
"""

import sys
import os
import time
import base64

# Add the current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from simulate_efento_sensor import EfentoSensorSimulator

# Import the payload generator
from test_efento_parser import generate_realistic_measurement_payload

try:
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

def test_sensor_variations(duration_minutes=5):
    """Test the sensor variations over a short period"""
    print(f"🧪 Testing sensor variations for {duration_minutes} minutes")
    
    # Create simulator
    simulator = EfentoSensorSimulator(
        measurement_interval=5,  # 5 second intervals for faster testing
        transmission_interval=30  # 30 second transmissions
    )
    
    # Collect data
    timestamps = []
    temperatures = []
    humidities = []
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    print(f"Initial values: T={simulator.temperature:.2f}°C, H={simulator.humidity:.2f}%")
    
    measurement_count = 0
    while time.time() < end_time:
        # Store previous values
        prev_temp = simulator.temperature
        prev_humidity = simulator.humidity
        
        # Simulate sensor reading
        simulator._simulate_sensor_readings()
        measurement_count += 1
        
        # Calculate changes
        temp_change = simulator.temperature - prev_temp
        humidity_change = simulator.humidity - prev_humidity
        
        # Store data
        timestamps.append(time.time() - start_time)
        temperatures.append(simulator.temperature)
        humidities.append(simulator.humidity)
        
        # Print every 10th measurement
        if measurement_count % 10 == 0:
            elapsed_min = (time.time() - start_time) / 60
            print(f"  {elapsed_min:.1f}min: T={simulator.temperature:.2f}°C (Δ{temp_change:+.2f}), H={simulator.humidity:.2f}% (Δ{humidity_change:+.2f})")
        
        time.sleep(1)  # 1 second intervals for testing
    
    # Calculate statistics
    temp_min, temp_max = min(temperatures), max(temperatures)
    temp_range = temp_max - temp_min
    humidity_min, humidity_max = min(humidities), max(humidities)
    humidity_range = humidity_max - humidity_min
    
    print(f"\n📊 Test Results:")
    print(f"  Measurements taken: {measurement_count}")
    print(f"  Temperature range: {temp_min:.2f}°C to {temp_max:.2f}°C (range: {temp_range:.2f}°C)")
    print(f"  Humidity range: {humidity_min:.2f}% to {humidity_max:.2f}% (range: {humidity_range:.2f}%)")
    
    # Test payload generation with sample values
    print(f"\n🧪 Testing Enhanced Payload Generator:")
    try:
        # Test with sample temperature and humidity
        test_temp = temperatures[len(temperatures)//2] if temperatures else 22.5
        test_humidity = humidities[len(humidities)//2] if humidities else 55.0
        
        payload_data = generate_realistic_measurement_payload(
            device_serial="282c0242526f",
            measurement_types=["TEMPERATURE", "HUMIDITY"],
            custom_temperature=test_temp,
            custom_humidity=test_humidity
        )
        
        print(f"  ✅ Payload generated successfully")
        print(f"  📦 Payload size: {len(payload_data['payload_bytes'])} bytes")
        print(f"  🌡️  Test temperature: {test_temp:.2f}°C")
        print(f"  💧 Test humidity: {test_humidity:.2f}%")
        
    except Exception as e:
        print(f"  ❌ Payload generation failed: {e}")
    
    # Create a simple plot if matplotlib is available
    if MATPLOTLIB_AVAILABLE:
        try:
            plt.figure(figsize=(12, 8))
            
            # Temperature plot
            plt.subplot(2, 1, 1)
            plt.plot(np.array(timestamps)/60, temperatures, 'b-', linewidth=2, label='Temperature')
            plt.title('Sensor Value Variations Over Time')
            plt.ylabel('Temperature (°C)')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # Humidity plot
            plt.subplot(2, 1, 2)
            plt.plot(np.array(timestamps)/60, humidities, 'g-', linewidth=2, label='Humidity')
            plt.xlabel('Time (minutes)')
            plt.ylabel('Humidity (%)')
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            plt.tight_layout()
            plot_file = '/tmp/sensor_variations_test.png'
            plt.savefig(plot_file, dpi=150, bbox_inches='tight')
            print(f"  📈 Plot saved to: {plot_file}")
            
        except Exception as e:
            print(f"  📈 Plot generation failed: {e}")
    else:
        print("  📈 matplotlib not available - no plot generated")
    
    # Check if variations are sufficient
    if temp_range < 0.5:
        print("  ⚠️  WARNING: Temperature variations are very small!")
    else:
        print("  ✅ Temperature variations look good")
        
    if humidity_range < 1.0:
        print("  ⚠️  WARNING: Humidity variations are very small!")
    else:
        print("  ✅ Humidity variations look good")
    
    return {
        'temperature_range': temp_range,
        'humidity_range': humidity_range,
        'measurements': measurement_count,
        'data': {
            'timestamps': timestamps,
            'temperatures': temperatures,
            'humidities': humidities
        }
    }

if __name__ == '__main__':
    test_sensor_variations() 