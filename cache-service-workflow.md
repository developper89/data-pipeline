# Cache Service Workflow

This document explains the simplified workflow of the metadata caching system, focusing on storing only the latest readings for each device.

## System Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Raw Message │─────▶ Normalizer  │─────▶   Kafka     │─────▶ Cache Service│
└─────────────┘     │  Service    │     │  (validated │     └──────┬──────┘
                   └─────────────┘     │    data)    │            │
                                       └─────────────┘            ▼
                                                              ┌─────────────┐
                                                              │    Redis    │
                                                              └─────────────┘
```

## Detailed Workflow

### 1. Normalizer Process

1. **Raw Message Intake**:

   - The normalizer service receives a raw message with a device ID and payload
   - Each raw message is assigned a unique `request_id`

2. **Parsing and Validation**:

   - The normalizer parses the raw message into multiple sensor readings
   - Each reading is individually validated
   - All readings from a single raw message share the same `request_id`

3. **Kafka Publishing**:
   - The normalizer publishes **each validated reading individually** to Kafka
   - Each Kafka message contains one `ValidatedOutput` object with:
     - `device_id`: The sensor's identifier
     - `metadata`: Information about the reading (datatype, unit, etc.)
     - `request_id`: Links related readings from the same raw message
     - `values`: The actual sensor reading values
     - `timestamp`: When the reading was taken

### 2. Cache Service Process

1. **Message Consumption**:

   - The cache service consumes `ValidatedOutput` messages from Kafka
   - For each message, it calls `cache_validated_output`

2. **Data Extraction**:

   - The service extracts key information from the `ValidatedOutput`:
     ```python
     device_id = validated_output.device_id
     metadata = validated_output.metadata
     request_id = validated_output.request_id
     ```

3. **Caching Flow**:
   - Check if this is a new request for this device (different request_id)
   - If it's a new request, clear previous readings for this device
   - Add the new reading to the device's readings list
   - Only the latest set of readings is stored - historical data is discarded

### 3. Redis Storage Structure

The caching system uses a simple, direct structure in Redis:

1. **Device Readings List**:

   - Key: `device:{device_id}:readings`
   - Value: List of JSON-serialized reading metadata from the latest request
   - Purpose: Store all readings for a device from its most recent request

2. **Current Request Tracker**:

   - Key: `device:{device_id}:current_request_id`
   - Value: The current request_id being processed
   - Purpose: Detect when a new request begins to clear previous readings

3. **Global Index**:
   - Key: `all_devices` (configured via settings)
   - Value: Set of all device IDs in the cache

### 4. Detailed Redis Operation Flow

When a `ValidatedOutput` is processed:

1. **Check if this is a new request**:

   ```
   GET device:{device_id}:current_request_id
   ```

2. **If request_id is different from stored one (or none exists)**:

   - Clear existing readings:
     ```
     DEL device:{device_id}:readings
     ```
   - Update current request:
     ```
     SET device:{device_id}:current_request_id {request_id}
     ```

3. **Add the reading to the device's readings list**:

   ```
   RPUSH device:{device_id}:readings {reading_json}
   ```

4. **Track the device**:

   ```
   SADD all_devices {device_id}
   ```

5. **Set expiration for keys**:
   ```
   EXPIRE device:{device_id}:readings {ttl}
   EXPIRE device:{device_id}:current_request_id {ttl}
   ```

### 5. Data Retrieval Methods

The caching service provides a single, direct method to access the cached data:

- **Get Device Readings**:

  - Method: `get_device_readings(device_id)`
  - Returns: List of all readings for a device from its most recent request
  - Implementation:
    ```python
    readings_key = f"device:{device_id}:readings"
    reading_jsons = await redis.lrange(readings_key, 0, -1)
    return [json.loads(r) for r in reading_jsons if r]
    ```
  - Use case: Get all current readings for a specific device

- **List All Devices**:
  - Method: `get_all_device_ids()`
  - Returns: List of all device IDs
  - Use case: System-wide exploration

## Example Workflow

Here's a concrete example:

1. A weather station (device_id: "station_123") sends a raw message with multiple sensor readings
2. The normalizer assigns request_id "req_456" and produces three readings:

   - Temperature: 25.5°C
   - Humidity: 65%
   - Wind speed: 10 km/h

3. The normalizer publishes three separate `ValidatedOutput` objects to Kafka:

   ```python
   ValidatedOutput(
     device_id="station_123",
     request_id="req_456",
     metadata={
       "datatype_id": "temp_sensor",
       "datatype_name": "Temperature",
       "datatype_unit": "Celsius"
     },
     values=[25.5]
   )

   ValidatedOutput(
     device_id="station_123",
     request_id="req_456",
     metadata={
       "datatype_id": "humidity_sensor",
       "datatype_name": "Humidity",
       "datatype_unit": "%"
     },
     values=[65]
   )

   ValidatedOutput(
     device_id="station_123",
     request_id="req_456",
     metadata={
       "datatype_id": "wind_sensor",
       "datatype_name": "Wind Speed",
       "datatype_unit": "km/h"
     },
     values=[10]
   )
   ```

4. The cache service processes each message:

   - First reading:
     - Checks stored request_id for "station_123" (none exists)
     - Sets current_request_id to "req_456"
     - Adds temperature reading to the readings list
   - Second reading:
     - Checks stored request_id (it's "req_456")
     - Adds humidity reading to the readings list
   - Third reading:
     - Checks stored request_id (it's "req_456")
     - Adds wind speed reading to the readings list

5. After processing, Redis contains:

   - `device:station_123:current_request_id = "req_456"`
   - `device:station_123:readings` = [temperature_data, humidity_data, wind_speed_data]

6. Later, when a new message arrives with request_id "req_789":

   - The service detects a different request_id
   - Clears the existing readings list
   - Starts adding new readings to the list
   - Updates current_request_id to "req_789"

7. Data retrieval is simple:
   - `get_device_readings("station_123")` returns all readings from the most recent request

## Use Cases

1. **Current Device Readings**:

   - API endpoint to get all current readings for a device
   - Retrieves all sensor data points from the latest request
   - Useful for dashboards and monitoring

2. **System Exploration**:
   - List all devices in the system
   - Quickly identify which devices have data available

## Implementation Considerations

1. **Memory Efficiency**:

   - Only the latest readings for each device are stored
   - Old readings are automatically purged when new requests arrive
   - No historical data is maintained

2. **Simplicity**:

   - Direct access model without complex data relationships
   - Single primary retrieval method makes API usage straightforward
   - No need to track multiple request IDs

3. **Performance**:
   - O(1) check if this is a new request
   - O(1) add to readings list
   - O(N) retrieve all readings for a device (where N is number of readings)

## Summary

This simplified workflow provides:

1. Storage of only the latest readings for each device
2. Automatic clearing of old readings when new ones arrive
3. Direct access to all readings without indirection
4. A clean, simple Redis structure
5. Efficient memory usage by avoiding historical data

This approach prioritizes simplicity and current state over historical tracking, making it efficient and straightforward to implement and use.
