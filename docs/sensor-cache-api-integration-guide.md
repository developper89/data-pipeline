# Sensor Cache API Integration Guide for UI Agents

## 📋 Overview

This guide provides comprehensive instructions for UI agents on how to retrieve cached sensor readings using the new **index-based caching system**. The system allows independent caching of different data types (e.g., configuration parameters, measurements) per device.

### Key Features

- **Index-based caching**: Different data types are cached independently by their index (e.g., "P", "U", "T")
- **Independent retrieval**: You can get the latest "P" readings without affecting "U" readings
- **No data loss**: New "P" readings don't overwrite "U" readings and vice versa
- **Request tracking**: Each index maintains its own request ID for data lineage

## 🔐 Authentication

All sensor cache endpoints require authentication. Use the standard Bearer token authentication.

### Getting Authentication Token

```bash
TOKEN=$(curl -s -X POST "http://localhost:8888/realms/Preservarium/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=preservarium-app" \
  -d "username=mafabrique" \
  -d "password=preservarium" | jq -r '.access_token')
```

### Authentication Headers

```bash
Authorization: Bearer ${TOKEN}
```

## 🌐 Available Endpoints

| Endpoint                                               | Method | Description                        |
| ------------------------------------------------------ | ------ | ---------------------------------- |
| `/api/v1/sensor-cache/devices/`                        | GET    | Get all cached device IDs          |
| `/api/v1/sensor-cache/{device_id}/indexes/`            | GET    | Get available indexes for a device |
| `/api/v1/sensor-cache/{device_id}/readings/`           | GET    | Get all readings grouped by index  |
| `/api/v1/sensor-cache/{device_id}/readings/{index}/`   | GET    | Get readings for specific index    |
| `/api/v1/sensor-cache/{device_id}/request-id/`         | GET    | Get request IDs for all indexes    |
| `/api/v1/sensor-cache/{device_id}/request-id/{index}/` | GET    | Get request ID for specific index  |

## 📊 Data Types and Indexes

### Common Index Types

- **"P"**: Configuration parameters (Paramètres)
- **"U"**: Measurements like humidity (Units/Mesures)
- **"T"**: Temperature measurements
- **"M"**: Other measurement types
- **Custom indexes**: Device-specific data types

### Data Structure

Each reading contains:

```json
{
  "device_id": "282c02424eed",
  "values": ["value1", "value2"],
  "label": ["label1", "label2"],
  "index": "P",
  "metadata": {
    "datatype_name": "Paramètres Texte",
    "datatype_unit": "%",
    "persist": true
  },
  "timestamp": "2025-06-25T08:57:34.618082+00:00",
  "request_id": "4bcf0f94-2850-48b0-8941-babcb54be305"
}
```

## 🛠️ Step-by-Step Integration Workflow

### Step 1: Discover Available Devices

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/devices/" | jq
```

**Response Example:**

```json
["282c02424eed", "2207001"]
```

### Step 2: Get Available Indexes for a Device

```bash
DEVICE_ID="282c02424eed"
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/${DEVICE_ID}/indexes/" | jq
```

**Response Example:**

```json
["P", "T", "U"]
```

### Step 3: Choose Your Retrieval Strategy

#### Option A: Get All Readings (Grouped by Index)

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/${DEVICE_ID}/readings/" | jq
```

**Response Structure:**

```json
{
  "P": [
    {
      "device_id": "282c02424eed",
      "values": ["5", "120"],
      "label": ["measurement_period_base", "transmission_interval"],
      "index": "P",
      "metadata": {...},
      "timestamp": "2025-06-25T08:57:34.618082+00:00",
      "request_id": "4bcf0f94-2850-48b0-8941-babcb54be305"
    }
  ],
  "U": [...],
  "T": [...]
}
```

#### Option B: Get Specific Index Readings

```bash
# Get only configuration parameters
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/${DEVICE_ID}/readings/P/" | jq

# Get only humidity measurements
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/${DEVICE_ID}/readings/U/" | jq
```

### Step 4: Handle the Response Data

#### Processing Grouped Readings (Option A)

```javascript
// Example JavaScript processing
const allReadings = await fetch(`/api/v1/sensor-cache/${deviceId}/readings/`, {
  headers: { Authorization: `Bearer ${token}` },
}).then((r) => r.json());

// Process each index separately
const configParams = allReadings.P || [];
const humidityData = allReadings.U || [];
const temperatureData = allReadings.T || [];

// Display in UI sections
displayConfigParams(configParams);
displayHumidityChart(humidityData);
displayTemperatureChart(temperatureData);
```

#### Processing Specific Index (Option B)

```javascript
// Get only what you need
const configParams = await fetch(
  `/api/v1/sensor-cache/${deviceId}/readings/P/`,
  {
    headers: { Authorization: `Bearer ${token}` },
  }
).then((r) => r.json());

displayConfigParams(configParams);
```

## 💡 UI Implementation Patterns

### Pattern 1: Dashboard with Separate Sections

```javascript
async function loadDeviceDashboard(deviceId) {
  try {
    // Get all data types available
    const indexes = await getDeviceIndexes(deviceId);

    // Load each section independently
    if (indexes.includes("P")) {
      const configData = await getReadingsByIndex(deviceId, "P");
      updateConfigSection(configData);
    }

    if (indexes.includes("U")) {
      const humidityData = await getReadingsByIndex(deviceId, "U");
      updateHumidityChart(humidityData);
    }

    if (indexes.includes("T")) {
      const tempData = await getReadingsByIndex(deviceId, "T");
      updateTemperatureChart(tempData);
    }
  } catch (error) {
    handleError(error);
  }
}
```

### Pattern 2: Lazy Loading by Data Type

```javascript
// Load data only when user clicks on a tab/section
async function onTabClick(deviceId, dataType) {
  const tabContent = document.getElementById(`${dataType}-content`);

  if (!tabContent.hasAttribute("data-loaded")) {
    showLoading(tabContent);

    try {
      const readings = await getReadingsByIndex(deviceId, dataType);
      renderReadings(tabContent, readings);
      tabContent.setAttribute("data-loaded", "true");
    } catch (error) {
      showError(tabContent, error);
    }
  }
}
```

### Pattern 3: Real-time Updates

```javascript
// Poll for updates on specific data types
async function startPolling(deviceId, indexes) {
  const pollInterval = 30000; // 30 seconds

  for (const index of indexes) {
    setInterval(async () => {
      try {
        const readings = await getReadingsByIndex(deviceId, index);
        updateUISection(index, readings);
      } catch (error) {
        console.error(`Error updating ${index} data:`, error);
      }
    }, pollInterval);
  }
}
```

## 🔍 Data Validation and Metadata

### Understanding Metadata

Each reading includes rich metadata:

```json
{
  "metadata": {
    "datatype_id": "5e090fde-b3e5-44eb-9e86-01ae4f2356e3",
    "datatype_name": "Humidité",
    "datatype_unit": "%",
    "persist": true,
    "battery_ok": true,
    "channel_number": 2,
    "source_sn_hex": "282c02424eed",
    "is_config_parameter": true
  }
}
```

### Using Metadata for UI Decisions

```javascript
function processReading(reading) {
  const { metadata, values, label } = reading;

  // Determine UI display based on metadata
  if (metadata.is_config_parameter) {
    return renderConfigParameter(reading);
  }

  // Use unit for formatting
  if (metadata.datatype_unit === "%") {
    return renderPercentageChart(values, label);
  }

  // Check data persistence
  if (metadata.persist) {
    return renderHistoricalData(reading);
  }

  return renderGenericReading(reading);
}
```

## ⚠️ Error Handling

### Common Error Scenarios

#### 1. Device Not Found (404)

```bash
# Response: 404 Not Found
{
  "detail": "No cached readings found for device unknown_device"
}
```

#### 2. Index Not Found (404)

```bash
# Response: 404 Not Found
{
  "detail": "No cached readings found for device 282c02424eed with index X"
}
```

#### 3. Authentication Error (401)

```bash
# Response: 401 Unauthorized
{
  "detail": "Not authenticated"
}
```

### Error Handling Pattern

```javascript
async function getReadingsByIndex(deviceId, index) {
  try {
    const response = await fetch(
      `/api/v1/sensor-cache/${deviceId}/readings/${index}/`,
      { headers: { Authorization: `Bearer ${token}` } }
    );

    if (!response.ok) {
      if (response.status === 404) {
        console.warn(`No ${index} readings found for device ${deviceId}`);
        return [];
      }
      if (response.status === 401) {
        // Refresh token and retry
        await refreshAuthToken();
        return getReadingsByIndex(deviceId, index);
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return await response.json();
  } catch (error) {
    console.error("Error fetching readings:", error);
    throw error;
  }
}
```

## 🚀 Performance Optimization

### Best Practices

#### 1. Cache Responses Locally

```javascript
const localCache = new Map();

async function getCachedReadings(deviceId, index) {
  const cacheKey = `${deviceId}:${index}`;
  const cached = localCache.get(cacheKey);

  if (cached && !isExpired(cached.timestamp)) {
    return cached.data;
  }

  const fresh = await getReadingsByIndex(deviceId, index);
  localCache.set(cacheKey, {
    data: fresh,
    timestamp: Date.now(),
  });

  return fresh;
}
```

#### 2. Parallel Loading

```javascript
async function loadMultipleIndexes(deviceId, indexes) {
  const promises = indexes.map((index) =>
    getReadingsByIndex(deviceId, index)
      .then((data) => ({ index, data }))
      .catch((error) => ({ index, error }))
  );

  const results = await Promise.all(promises);

  // Process results
  for (const result of results) {
    if (result.error) {
      console.error(`Failed to load ${result.index}:`, result.error);
    } else {
      updateUISection(result.index, result.data);
    }
  }
}
```

#### 3. Request Deduplication

```javascript
const activeRequests = new Map();

async function dedupedRequest(url) {
  if (activeRequests.has(url)) {
    return activeRequests.get(url);
  }

  const promise = fetch(url).then((r) => r.json());
  activeRequests.set(url, promise);

  try {
    const result = await promise;
    activeRequests.delete(url);
    return result;
  } catch (error) {
    activeRequests.delete(url);
    throw error;
  }
}
```

## 📱 Mobile/Responsive Considerations

### Data Prioritization for Mobile

```javascript
// Load critical data first on mobile
async function loadMobileOptimized(deviceId) {
  const isMobile = window.innerWidth < 768;

  if (isMobile) {
    // Load only essential data first
    const criticalIndexes = await getCriticalIndexes(deviceId);
    for (const index of criticalIndexes) {
      await loadAndRender(deviceId, index);
    }

    // Load remaining data in background
    setTimeout(() => loadRemainingData(deviceId), 1000);
  } else {
    // Load all data for desktop
    await loadAllData(deviceId);
  }
}
```

## 🔧 Testing Your Integration

### Test Checklist

- [ ] ✅ Can retrieve list of cached devices
- [ ] ✅ Can get available indexes for a device
- [ ] ✅ Can fetch all readings grouped by index
- [ ] ✅ Can fetch specific index readings
- [ ] ✅ Handles 404 errors gracefully
- [ ] ✅ Handles authentication errors
- [ ] ✅ UI updates correctly with new data
- [ ] ✅ Performance is acceptable on mobile
- [ ] ✅ Local caching works properly
- [ ] ✅ Real-time updates function correctly

### Test Script Example

```bash
#!/bin/bash
# Test script for sensor cache integration

echo "🔐 Getting authentication token..."
TOKEN=$(curl -s -X POST "http://localhost:8888/realms/Preservarium/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=preservarium-app" \
  -d "username=mafabrique" \
  -d "password=preservarium" | jq -r '.access_token')

echo "📱 Testing device discovery..."
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/devices/" | jq

echo "🔍 Testing index discovery..."
DEVICE_ID=$(curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/devices/" | jq -r '.[0]')

curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/${DEVICE_ID}/indexes/" | jq

echo "📊 Testing data retrieval..."
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:3333/api/v1/sensor-cache/${DEVICE_ID}/readings/P/" | jq '. | length'

echo "✅ All tests completed!"
```

## 📞 Support

For integration issues or questions:

1. Check this documentation first
2. Test with the provided curl commands
3. Verify authentication tokens are valid
4. Check server logs for detailed error messages
5. Contact the backend team with specific error details

---

**🎯 Remember**: The key advantage of this system is **independent caching by index**. You can now retrieve "P" readings and "U" readings separately without them interfering with each other!
