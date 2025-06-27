#!/usr/bin/env python3
"""
Efento Sensor Simulator
Simulates a real Efento sensor by sending CoAP data every minute
"""

import time
import random
import subprocess
import sys
import os
import math
from datetime import datetime, timezone
import base64

# Import our test payload generator
from test_efento_parser import generate_realistic_measurement_payload

class EfentoSensorSimulator:
    def __init__(self, 
                 coap_server_host="localhost", 
                 coap_server_port=5683,
                 device_serial="KCwCQlJv",  # Base64 encoded "282c0242526f"
                 measurement_interval=60,  # seconds
                 transmission_interval=120):  # seconds
        
        self.coap_server_host = coap_server_host
        self.coap_server_port = coap_server_port
        self.device_serial = device_serial
        self.measurement_interval = measurement_interval
        self.transmission_interval = transmission_interval
        
        # Sensor state
        self.temperature = 20.0  # Starting temperature
        self.humidity = 50.0     # Starting humidity
        self.battery_ok = True
        self.measurement_count = 0
        self.last_transmission = 0
        
        # Use aiocoap-client directly
        self.coap_client = "aiocoap-client"
        
        # Track simulation start time for realistic patterns
        self.start_time = time.time()
        

    
    def _simulate_sensor_readings(self):
        """Simulate realistic sensor readings with noticeable variations"""
        # Create more noticeable changes over shorter time periods
        
        # Use elapsed time from start for more predictable patterns
        elapsed_minutes = (time.time() - getattr(self, 'start_time', time.time())) / 60
        
        # Temperature simulation with multiple components:
        # 1. Slow trending (over 30 minutes)
        temp_trend = 3.0 * math.sin(elapsed_minutes / 30 * 2 * math.pi)
        
        # 2. Medium oscillation (over 5 minutes) 
        temp_medium = 2.0 * math.sin(elapsed_minutes / 5 * 2 * math.pi)
        
        # 3. Random noise for realism
        temp_noise = random.uniform(-1.0, 1.0)
        
        # 4. Small random walk component
        temp_walk = random.uniform(-0.5, 0.5)
        
        # Apply changes with proper scaling
        temp_change = temp_trend * 0.3 + temp_medium * 0.2 + temp_noise * 0.3 + temp_walk
        self.temperature += temp_change
        
        # Clamp to realistic range
        self.temperature = max(10.0, min(35.0, self.temperature))
        
        # Humidity simulation with inverse correlation to temperature
        # Base humidity change inversely related to temperature
        humidity_temp_inverse = -temp_change * 1.5
        
        # Independent humidity oscillation (over 7 minutes)
        humidity_oscillation = 5.0 * math.sin(elapsed_minutes / 7 * 2 * math.pi)
        
        # Humidity noise
        humidity_noise = random.uniform(-2.0, 2.0)
        
        # Random walk for humidity
        humidity_walk = random.uniform(-1.0, 1.0)
        
        # Apply humidity changes
        humidity_change = humidity_temp_inverse + humidity_oscillation * 0.2 + humidity_noise * 0.3 + humidity_walk * 0.2
        self.humidity += humidity_change
        
        # Clamp humidity to realistic range
        self.humidity = max(20.0, min(90.0, self.humidity))
        
        # Occasionally simulate sensor errors (2% chance - reduced frequency)
        if random.random() < 0.02:
            return "error"
        
        return "normal"
    
    def _generate_measurement_payload(self):
        """Generate a realistic measurement payload with current sensor values"""
        try:
            # Convert device serial from base64 to hex if needed
            device_serial_hex = self.device_serial
            if len(self.device_serial) == 8:  # Base64 format like "KCwCQlJv"
                device_serial_hex = base64.b64decode(self.device_serial).hex()
            
            # Generate payload with current sensor values
            payload_data = generate_realistic_measurement_payload(
                device_serial=device_serial_hex,
                include_errors=(random.random() < 0.02),  # 2% chance of errors
                measurement_types=["TEMPERATURE", "HUMIDITY"],
                custom_temperature=self.temperature,
                custom_humidity=self.humidity
            )
            return payload_data['payload_bytes']
            
        except Exception as e:
            print(f"Warning: Enhanced payload generator failed: {e}")
            return False
    
    def _generate_basic_payload(self):
        """Generate a basic payload when the generator isn't available"""
        # Create a minimal protobuf-like payload
        # This is a simplified version for when the full generator isn't available
        import struct
        
        # Basic structure: [serial][timestamp][temp][humidity][battery]
        serial_bytes = base64.b64decode(self.device_serial)
        timestamp = int(time.time())
        temp_int = int(self.temperature * 10)
        humidity_int = int(self.humidity)
        battery_byte = 1 if self.battery_ok else 0
        
        payload = serial_bytes + struct.pack('>IHHB', timestamp, temp_int, humidity_int, battery_byte)
        return payload
    
    def _send_coap_message(self, payload, endpoint="/m"):
        """Send CoAP message to server using aiocoap-client"""
        # Create temporary file for payload
        payload_file = f"/tmp/efento_payload_{int(time.time())}.bin"
        try:
            with open(payload_file, 'wb') as f:
                f.write(payload)
            
            # Construct aiocoap-client command
            coap_url = f"coap://{self.coap_server_host}:{self.coap_server_port}{endpoint}"
            cmd = [
                "aiocoap-client",
                "-m", "POST",
                "--payload", f"@{payload_file}",
                "--content-format", "application/octet-stream",
                coap_url
            ]
            
            print(f"📡 Sending CoAP POST to {coap_url}")
            print(f"   Payload size: {len(payload)} bytes")
            
            # Execute CoAP command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                print(f"✅ CoAP message sent successfully")
                if result.stdout:
                    print(f"   Response: {result.stdout.strip()}")
                return True
            else:
                print(f"❌ CoAP message failed (exit code: {result.returncode})")
                if result.stderr:
                    print(f"   Error: {result.stderr.strip()}")
                if result.stdout:
                    print(f"   Output: {result.stdout.strip()}")
                return False
                
        except Exception as e:
            print(f"❌ Error sending CoAP message: {e}")
            return False
        finally:
            # Clean up temporary file
            if os.path.exists(payload_file):
                os.remove(payload_file)
    
    def _send_device_info(self):
        """Send device info message using protobuf generator"""
        print("📋 Sending device info...")
        try:
            from test_efento_parser import generate_realistic_device_info_payload
            
            # Convert device serial from base64 to hex if needed
            device_serial_hex = self.device_serial
            if len(self.device_serial) == 8:  # Base64 format like "KCwCQlJv"
                device_serial_hex = base64.b64decode(self.device_serial).hex()
            
            # Generate realistic device info payload using protobuf
            device_info_payload = generate_realistic_device_info_payload(device_serial_hex)
            payload = device_info_payload['payload_bytes']
            print(f"   📦 Using protobuf device info payload ({len(payload)} bytes)")
            
            self._send_coap_message(payload, "/i")
            
        except Exception as e:
            print(f"❌ Device info generation failed: {e}")
    
    def _send_device_config(self):
        """Send device configuration message using protobuf generator"""
        print("⚙️ Sending device configuration...")
        try:
            from test_efento_parser import generate_realistic_config_payload
            
            # Convert device serial from base64 to hex if needed
            device_serial_hex = self.device_serial
            if len(self.device_serial) == 8:  # Base64 format like "KCwCQlJv"
                device_serial_hex = base64.b64decode(self.device_serial).hex()
            
            # Generate realistic config payload using protobuf
            config_payload = generate_realistic_config_payload(device_serial_hex)
            payload = config_payload['payload_bytes']
            print(f"   📦 Using protobuf config payload ({len(payload)} bytes)")
            
            self._send_coap_message(payload, "/c")
            
        except Exception as e:
            print(f"❌ Config generation failed: {e}")
    
    def run_simulation(self, duration_minutes=None):
        """Run the sensor simulation"""
        print(f"🚀 Starting Efento sensor simulation")
        print(f"   Target: {self.coap_server_host}:{self.coap_server_port}")
        print(f"   Device Serial: {self.device_serial}")
        print(f"   Measurement interval: {self.measurement_interval}s")
        print(f"   Transmission interval: {self.transmission_interval}s")
        
        if duration_minutes:
            print(f"   Duration: {duration_minutes} minutes")
            end_time = time.time() + (duration_minutes * 60)
        else:
            print(f"   Duration: Indefinite (Ctrl+C to stop)")
            end_time = None
        
        print("=" * 50)
        
        start_time = time.time()
        next_measurement = start_time
        next_transmission = start_time
        
        try:
            while True:
                current_time = time.time()
                
                # Check if simulation should end
                if end_time and current_time >= end_time:
                    print("⏰ Simulation duration completed")
                    break
                
                # Take measurement
                if current_time >= next_measurement:
                    # Store previous values to calculate changes
                    prev_temp = self.temperature
                    prev_humidity = self.humidity
                    
                    sensor_status = self._simulate_sensor_readings()
                    self.measurement_count += 1
                    
                    # Calculate actual changes
                    temp_change = self.temperature - prev_temp
                    humidity_change = self.humidity - prev_humidity
                    
                    timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    print(f"📊 [{timestamp}] Measurement #{self.measurement_count}")
                    print(f"   Temperature: {self.temperature:.2f}°C (Δ{temp_change:+.2f})")
                    print(f"   Humidity: {self.humidity:.2f}% (Δ{humidity_change:+.2f})")
                    print(f"   Status: {sensor_status}")
                    
                    next_measurement = current_time + self.measurement_interval
                
                # Send all three endpoints simultaneously
                if current_time >= next_transmission:
                    print(f"📡 Sending complete sensor data package...")
                    
                    # Send measurements (/m)
                    payload_m = self._generate_measurement_payload()
                    success_m = self._send_coap_message(payload_m, "/m")
                    
                    # Send device info (/i)
                    self._send_device_info()
                    
                    # Send device config (/c)
                    self._send_device_config()
                    
                    if success_m:
                        self.last_transmission = current_time
                    
                    next_transmission = current_time + self.transmission_interval
                
                # Sleep until next event
                next_event = min(next_measurement, next_transmission)
                sleep_time = max(0.1, next_event - current_time)
                time.sleep(min(sleep_time, 1.0))  # Sleep max 1 second at a time
                
        except KeyboardInterrupt:
            print("\n🛑 Simulation stopped by user")
        except Exception as e:
            print(f"\n❌ Simulation error: {e}")
            import traceback
            traceback.print_exc()
        
        elapsed = time.time() - start_time
        print(f"\n📈 Simulation Summary:")
        print(f"   Runtime: {elapsed/60:.1f} minutes")
        print(f"   Measurements taken: {self.measurement_count}")
        print(f"   Last transmission: {time.time() - self.last_transmission:.1f}s ago")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Efento Sensor Simulator")
    parser.add_argument("--host", default="localhost", help="CoAP server host")
    parser.add_argument("--port", type=int, default=5683, help="CoAP server port")
    parser.add_argument("--serial", default="KCwCQlJv", help="Device serial (base64)")
    parser.add_argument("--measurement-interval", type=int, default=60, 
                       help="Measurement interval in seconds")
    parser.add_argument("--transmission-interval", type=int, default=120,
                       help="Transmission interval in seconds")
    parser.add_argument("--duration", type=int, help="Duration in minutes (default: infinite)")
    
    args = parser.parse_args()
    
    simulator = EfentoSensorSimulator(
        coap_server_host=args.host,
        coap_server_port=args.port,
        device_serial=args.serial,
        measurement_interval=args.measurement_interval,
        transmission_interval=args.transmission_interval
    )
    
    simulator.run_simulation(duration_minutes=args.duration)


if __name__ == "__main__":
    main() 