# Light Controller Unified Cache Reading Implementation Plan

## 🎯 Objective

Implement a unified cache reading system for light controller devices that maintains a single cache entry with all datatype labels, accessible through `metadata.readings`, and updates this reading based on different device operations.

## 📋 Current State Analysis

### Existing Parser Behavior
- **File**: `data-pipeline/storage/parser_scripts/light_controller_parser.py`
- **Current Issue**: Creates individual readings per operation (task-specific readings)
- **Datatype Reference**: ID `7fe17021-fe10-4932-8faf-f26756701e9e` with 12 labels for category "L"

### Datatype Schema (Category "L")
```python
{
    "labels": [
        "task_time_beg", "task_duration", "task_day", "task_channel", 
        "task_active", "task_run_once", "lux_rate_c1", "lux_rate_c2", 
        "lux_name_c1", "lux_name_c2", "lux_per_month", "task_count"
    ],
    "display_names": [
        "Tâche - Heure début", "Tâche - Durée (min)", "Tâche - Jour", 
        "Tâche - Canal", "Tâche - Actif", "Tâche - Unique", 
        "Canal 1 - Taux lux/h", "Canal 2 - Taux lux/h", "Canal 1 - Nom", 
        "Canal 2 - Nom", "Quota mensuel lux", "Nombre de tâches"
    ],
    "data_type": [
        "integer", "integer", "integer", "integer", "boolean", "boolean",
        "integer", "integer", "string", "string", "integer", "integer"
    ]
}
```

## 🏗️ Implementation Plan

### Phase 1: Create Unified Reading Manager

#### 1.1 New UnifiedReadingManager Class
Create a new class to manage the unified cache reading:

```python
class UnifiedReadingManager:
    """Manages a single unified cache reading for light controller devices."""
    
    @staticmethod
    def create_base_reading(device_id: str, metadata: dict) -> dict:
        """Create initial unified reading with all datatype labels."""
        
    @staticmethod
    def update_reading_from_operation(base_reading: dict, operation_data: dict, operation_type: str) -> dict:
        """Update unified reading based on operation type and data."""
        
    @staticmethod
    def get_reading_from_metadata(metadata: dict, device_id: str) -> Optional[dict]:
        """Retrieve existing reading from metadata.readings or create new one."""
```

#### 1.2 Reading Structure
The unified reading will contain all 12 datatype labels with task-related values as lists:

```python
{
    "device_id": "2121004",
    "values": [
        [0] * 15,      # task_time_beg: List of 15 task start times
        [0] * 15,      # task_duration: List of 15 task durations  
        [0] * 15,      # task_day: List of 15 task days
        [0] * 15,      # task_channel: List of 15 task channels
        [False] * 15,  # task_active: List of 15 task active states
        [False] * 15,  # task_run_once: List of 15 task run-once flags
        0,             # lux_rate_c1: Single value
        0,             # lux_rate_c2: Single value
        "",            # lux_name_c1: Single value
        "",            # lux_name_c2: Single value
        0,             # lux_per_month: Single value
        0              # task_count: Single value
    ],
    "labels": ["task_time_beg", "task_duration", ..., "task_count"],      # 12 labels
    "display_names": ["Tâche - Heure début", ..., "Nombre de tâches"],   # 12 display names
    "index": "L",
    "timestamp": "2025-07-25T12:23:01.550155",
    "metadata": {
        "operation": "read_all",
        "task_count": 7,
        # ... other metadata
    }
}
```

### Phase 2: Operation-Specific Update Logic

#### 2.1 Operation Handlers Modification
Update each operation handler to work with the unified reading:

**2.1.1 `handle_read_all`**
- Retrieve/create unified reading from `metadata.readings`
- Update `task_count` value (index 11)
- Keep other values unchanged unless device provides them
- Return single unified reading

**2.1.2 `handle_create_task`**  
- Retrieve/create unified reading from `metadata.readings`
- Update task lists at specific task_id index:
  - `values[0][task_id] = time_beg` (task_time_beg list)
  - `values[1][task_id] = duration` (task_duration list)
  - `values[2][task_id] = day` (task_day list)
  - etc.
- Update `task_count` if new task created
- Return single unified reading

**2.1.3 `handle_read_task`**
- Retrieve/create unified reading from `metadata.readings`  
- Update task lists at specific task_id index with response data
- Return single unified reading

**2.1.4 `handle_delete_task` & `handle_delete_all`**
- Retrieve/create unified reading from `metadata.readings`
- For `delete_task`: Set task lists at task_id index to default values
- For `delete_all`: Reset all task lists to default values
- Update `task_count` accordingly (decrement or set to 0)
- Return single unified reading

#### 2.2 Value Mapping Strategy
Map operation-specific data to unified reading positions with task-related values as lists:

```python
# Value indices in unified reading array
VALUE_INDICES = {
    "task_time_beg": 0,    # List: [task0_time, task1_time, ..., task14_time] (15 max tasks)
    "task_duration": 1,    # List: [task0_duration, task1_duration, ..., task14_duration]
    "task_day": 2,         # List: [task0_day, task1_day, ..., task14_day]
    "task_channel": 3,     # List: [task0_channel, task1_channel, ..., task14_channel]
    "task_active": 4,      # List: [task0_active, task1_active, ..., task14_active]
    "task_run_once": 5,    # List: [task0_run_once, task1_run_once, ..., task14_run_once]
    "lux_rate_c1": 6,      # Single integer: Channel 1 lux rate
    "lux_rate_c2": 7,      # Single integer: Channel 2 lux rate  
    "lux_name_c1": 8,      # Single string: Channel 1 name
    "lux_name_c2": 9,      # Single string: Channel 2 name
    "lux_per_month": 10,   # Single integer: Monthly lux quota
    "task_count": 11       # Single integer: Total number of active tasks
}

# Task ID range: 0-14 (15 possible tasks as per datatype max constraint)
MAX_TASKS = 15
```

### Phase 3: Metadata Integration

#### 3.1 Reading Persistence in Metadata
Implement reading persistence through `metadata.readings`:

```python
def get_or_create_unified_reading(metadata: dict, device_id: str) -> dict:
    """Get existing reading from metadata or create new one."""
    if 'readings' not in metadata:
        metadata['readings'] = {}
    
    if 'L' not in metadata['readings']:
        metadata['readings']['L'] = create_base_reading(device_id, metadata)
    
    return metadata['readings']['L']
```

#### 3.2 Reading Update Flow
```python
def update_unified_reading(metadata: dict, device_id: str, operation_data: dict, operation_type: str) -> dict:
    """Update unified reading and store back in metadata."""
    current_reading = get_or_create_unified_reading(metadata, device_id)
    updated_reading = update_reading_from_operation(current_reading, operation_data, operation_type)
    metadata['readings']['L'] = updated_reading
    return updated_reading
```

### Phase 4: Parser Integration

#### 4.1 Main Parse Function Update
Modify the main `parse()` function to use unified reading approach:

```python
def parse(payload: str, metadata: dict, config: dict, message_parser=None) -> Optional[ParseResult]:
    """Updated parser using unified reading system."""
    # ... existing validation and decoding ...
    
    # Get/create unified reading from metadata
    unified_reading = UnifiedReadingManager.get_or_create_unified_reading(metadata, device_id)
    
    # Update reading based on operation
    updated_reading = UnifiedReadingManager.update_reading_from_operation(
        unified_reading, decoded, operation
    )
    
    # Store updated reading back in metadata
    metadata['readings'] = {'L': updated_reading}
    
    # Return single reading
    return ParseResult("data", [updated_reading])
```

#### 4.2 Operation Handler Refactoring
Refactor existing operation handlers to use unified approach:

```python
# Before: Multiple readings per operation
def handle_read_all(device_id, decoded, metadata):
    return [individual_task_reading_1, individual_task_reading_2, ...]

# After: Single unified reading
def handle_read_all(device_id, decoded, metadata):
    unified_reading = UnifiedReadingManager.update_unified_reading(
        metadata, device_id, decoded, 'read_all'
    )
    return [unified_reading]
```

## 🔄 Data Flow Changes

### Before (Current)
```
Command → Device Response → Parse → Multiple Task Readings → Cache (Multiple Entries)
```

### After (Proposed)
```
Command → Device Response → Parse → Single Unified Reading → Cache (Single Entry) 
                                 ↑
                          metadata.readings maintains state
```

## 🎯 Benefits

1. **Consistent Cache Structure**: Always 12 values matching datatype schema
2. **State Persistence**: Unified reading maintains device state across operations
3. **Simplified Frontend**: Single cache entry to query instead of multiple
4. **Operation Flexibility**: Any operation can update any part of the reading
5. **Data Integrity**: Complete picture of device state in one reading

## 🔧 Implementation Steps

### Step 1: Create UnifiedReadingManager Class
- Implement base reading creation with all 12 datatype labels
- Implement reading update logic for each operation type
- Add metadata integration functions

### Step 2: Update Operation Handlers  
- Modify each handler to use unified reading approach
- Implement value mapping from operation data to unified positions
- Ensure single reading return instead of multiple

### Step 3: Update Main Parse Function
- Integrate UnifiedReadingManager usage
- Modify return logic to always return single reading
- Add metadata.readings management

### Step 4: Testing & Validation
- Test each operation type with unified reading
- Verify cache entries match datatype schema
- Validate frontend compatibility

## ⚠️ Considerations

1. **Backward Compatibility**: Ensure frontend can handle single reading format
2. **Default Values**: Define appropriate defaults for each data type
3. **Error Handling**: Graceful handling when metadata.readings is corrupted
4. **Performance**: Minimal overhead from reading management
5. **Thread Safety**: Ensure metadata modifications are safe

## 📊 Expected Cache Result

After implementation, sensor cache will contain:
```json
{
  "L": [
    {
      "device_id": "2121004",
      "values": [
        [540, 600, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // task_time_beg (task 0=540, task 1=600, rest=0)
        [480, 120, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  // task_duration (task 0=480, task 1=120, rest=0)
        [2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // task_day (task 0=Tuesday, task 1=Wednesday, rest=0)
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      // task_channel (task 0=channel 0, task 1=channel 1, rest=0)
        [true, true, false, false, false, false, false, false, false, false, false, false, false, false, false],  // task_active (task 0&1=active, rest=inactive)
        [false, false, false, false, false, false, false, false, false, false, false, false, false, false, false], // task_run_once (all false)
        100,           // lux_rate_c1: Channel 1 lux rate
        150,           // lux_rate_c2: Channel 2 lux rate
        "Channel1",    // lux_name_c1: Channel 1 name
        "Channel2",    // lux_name_c2: Channel 2 name
        1000,          // lux_per_month: Monthly quota
        2              // task_count: 2 active tasks (tasks 0 and 1)
      ],
      "labels": ["task_time_beg", "task_duration", "task_day", "task_channel", "task_active", "task_run_once", "lux_rate_c1", "lux_rate_c2", "lux_name_c1", "lux_name_c2", "lux_per_month", "task_count"],
      "index": "L",
      "metadata": {
        "operation": "read_all",
        "task_count": 2
      }
    }
  ]
}
```

This approach provides a clean, maintainable solution that aligns with the datatype schema while preserving device state across operations.