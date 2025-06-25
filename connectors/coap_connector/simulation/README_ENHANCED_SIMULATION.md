# Enhanced Efento Sensor Simulation

This directory contains improved sensor simulation tools that generate realistic, varying temperature and humidity data for testing.

## Key Improvements

### 🔧 **Fixed Constant Values Issue**

The original simulation produced constant values due to:

- **Very small changes**: Temperature/humidity variations were barely noticeable (±0.1°C)
- **Slow patterns**: 24-hour sine waves appeared constant over short testing periods
- **Payload disconnect**: Generated values weren't properly encoded in protobuf payloads

### 📈 **Enhanced Sensor Simulation**

**`simulate_efento_sensor.py`** now features:

- **Multi-scale variations**: 30-min trends, 5-min oscillations, and random noise
- **Realistic ranges**: Temperature ±3°C, humidity ±5-8% variations
- **Natural patterns**: Inverse temperature-humidity correlation
- **Better monitoring**: Shows actual value changes (Δ) in real-time

### 🔌 **Centralized Payload Generation**

**`test_efento_parser.py`** enhanced with:

- **Custom sensor values**: `generate_realistic_measurement_payload()` now accepts:
  - `custom_temperature`: Current temperature in °C
  - `custom_humidity`: Current humidity in %
  - `custom_pressure`: Current pressure in hPa (optional)
- **Proper encoding**: Values correctly encoded in protobuf format
- **Backwards compatibility**: Existing test cases continue to work

## Files

### Core Simulation

- **`simulate_efento_sensor.py`**: Main sensor simulator with enhanced variations
- **`test_efento_parser.py`**: Protobuf payload generator with custom values support

### Testing & Verification

- **`test_sensor_variations.py`**: Verification script to test value variations
- **`test_efento_parser.py`**: Comprehensive parser testing suite

### Legacy Support

- **`debug_coap_client.py`**: Basic CoAP testing client
- **`verify_protobuf_setup.py`**: Protobuf environment verification

## Usage Examples

### 1. Basic Simulation with Enhanced Variations

```bash
# Run simulator with noticeable temperature/humidity changes
python simulate_efento_sensor.py --host localhost --duration 10
```

### 2. Custom Payload Generation

```python
from test_efento_parser import generate_realistic_measurement_payload

# Generate payload with specific sensor values
payload_data = generate_realistic_measurement_payload(
    device_serial="282c0242526f",
    measurement_types=["TEMPERATURE", "HUMIDITY"],
    custom_temperature=23.45,    # Current temperature
    custom_humidity=67.8         # Current humidity
)

# Get binary payload for CoAP transmission
binary_payload = payload_data['payload_bytes']
```

### 3. Test Value Variations

```bash
# Verify that sensor values are changing properly
python test_sensor_variations.py
```

Expected output:

```
🧪 Testing sensor variations for 5 minutes
Initial values: T=20.00°C, H=50.00%
  1.0min: T=21.34°C (Δ+0.73), H=48.21% (Δ-1.12)
  2.0min: T=22.18°C (Δ+0.84), H=46.93% (Δ-1.28)
...
📊 Test Results:
  Temperature range: 19.23°C to 24.67°C (range: 5.44°C)
  Humidity range: 42.15% to 56.78% (range: 14.63%)
  ✅ Temperature variations look good
  ✅ Humidity variations look good
```

## Key Functions

### Enhanced Payload Generator

```python
def generate_realistic_measurement_payload(
    device_serial: str = "282c0242526f",
    include_errors: bool = False,
    measurement_types: List[str] = None,
    custom_temperature: Optional[float] = None,
    custom_humidity: Optional[float] = None,
    custom_pressure: Optional[float] = None
) -> Dict[str, Any]:
```

### Sensor Simulation

```python
def _simulate_sensor_readings(self):
    """Generate realistic sensor variations using multiple time scales"""
    # 30-minute trends + 5-minute oscillations + noise + random walk
    # Results in temperature changes of ±1-3°C per measurement
    # Humidity inversely correlated with temperature changes
```

## Expected Database Results

With the enhanced simulation, you should now see:

### Temperature Data

- **Range**: 18-28°C typical variations
- **Pattern**: Smooth trends with realistic noise
- **Changes**: 0.5-2°C between measurements
- **Resolution**: 0.1°C precision

### Humidity Data

- **Range**: 35-70% typical variations
- **Pattern**: Inverse correlation with temperature
- **Changes**: 1-3% between measurements
- **Resolution**: 1% precision

## Troubleshooting

### Still Getting Constant Values?

1. **Check console output**: Look for Δ values showing changes
2. **Verify payload generation**: Run `test_sensor_variations.py`
3. **Check database ingestion**: Ensure timestamps are unique
4. **Increase variation**: Modify scaling factors in `_simulate_sensor_readings()`

### Payload Generation Issues?

1. **Verify protobuf**: Run `verify_protobuf_setup.py`
2. **Check imports**: Ensure `test_efento_parser` imports correctly
3. **Test manually**: Use enhanced payload generator directly

## Migration Notes

### From Original Simulator

- **Drop-in replacement**: Same command-line interface
- **Enhanced output**: More detailed console logging
- **Same endpoints**: `/m`, `/i`, `/c` still supported

### For Existing Tests

- **Backwards compatible**: Existing test cases work unchanged
- **Enhanced features**: Can now specify custom sensor values
- **Better debugging**: More informative error messages

The enhanced simulation should resolve the constant values issue while providing more realistic and useful test data for your IoT data pipeline.
