# Database Connection Pool Architecture

## 🏗️ **Overview**

Your system uses **TWO different database connection pool patterns** depending on the service type:

1. **Backend Services** (FastAPI) - **Per-request session pattern**
2. **Data Pipeline Services** - **Long-lived session pattern**

Both share the same underlying connection pool but manage sessions differently.

---

## 📍 **Connection Pool Creation Locations**

### **1. Data Pipeline Connection Pool**

**Location**: `shared/db/database.py`
**Used by**: normalizer, ingestor, caching, mailer services

```python
# POOL CREATION (Module-level, created once on import)
from preservarium_sdk.core.config import SettingsManager

settings = SettingsManager.get_settings()
db_settings = settings.db

# 🏊‍♂️ CONNECTION POOL CREATED HERE
async_engine = create_async_engine(
    database_url,
    pool_size=10,                    # Base connections
    max_overflow=20,                 # Additional connections when needed
    pool_timeout=30,                 # Wait time for connection
    pool_recycle=3600,              # Refresh connections every hour
    pool_pre_ping=True,             # Verify connections before use
    connect_args={...}              # Timeout configurations
)

# SESSION FACTORY (uses the pool)
async_session_factory = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
```

### **2. Backend Services Connection Pool**

**Location**: `backend/app/infrastructure/database/database.py`
**Used by**: python-backend FastAPI application

```python
# POOL CREATION (Module-level, created once on import)
from app.core.database_config import DatabaseConfig

db_config = get_database_config()  # From SDK SettingsManager

# 🏊‍♂️ CONNECTION POOL CREATED HERE
async_engine = create_database_engine(db_config)  # Same pool config as pipeline
async_session_factory = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
```

---

## 🔄 **How Connection Pools Are Used**

### **Pattern 1: Data Pipeline (Long-lived Sessions)**

**Used by**: normalizer, ingestor, caching, mailer

```python
# SERVICE INITIALIZATION (normalizer/main.py)
async with manage_database() as db_session:
    # 🔐 GET ONE SESSION FOR ENTIRE SERVICE LIFECYCLE
    db_session = await init_db()  # Returns AsyncSession from pool

    # 📦 INJECT SESSION INTO ALL REPOSITORIES
    datatype_repository = SQLDatatypeRepository(db_session)
    sensor_repository = SQLSensorRepository(db_session)
    hardware_repository = SQLHardwareRepository(db_session)
    # ... all repositories share the SAME session

    # 🚀 RUN SERVICE WITH SHARED SESSION
    service = NormalizerService(
        datatype_repository=datatype_repository,
        sensor_repository=sensor_repository,
        # ... repositories all use the same db_session
    )
    await service.run()  # Service runs for hours/days with same session
```

**Repository Usage**: All repositories share **ONE session** for the entire service lifecycle:

```python
# All these use the SAME session:
device = await self.sensor_repository.find_one_by(parameter=device_id)          # Session 1
hardware = await self.hardware_repository.get_hardware_by_sensor_parameter()   # Session 1
datatypes = await self.datatype_repository.get_sensor_datatypes_ordered()      # Session 1
```

### **Pattern 2: Backend Services (Per-Request Sessions)**

**Used by**: python-backend FastAPI endpoints

```python
# FASTAPI DEPENDENCY (backend/app/infrastructure/database/database.py)
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    # 🔐 GET NEW SESSION FOR EACH HTTP REQUEST
    session = async_session_factory()  # New session from pool

    try:
        yield session                   # Give to endpoint
        await session.commit()          # Commit on success
    except Exception as e:
        await session.rollback()        # Rollback on error
    finally:
        await session.close()           # Return to pool

# API ENDPOINT USAGE
@router.get("/sensors/search/")
async def search(
    parameter: str,
    sensor_use_case: SensorUseCase = Depends(get_sensor_use_case),  # Gets NEW session
):
    # Each HTTP request gets its own session
    return await sensor_use_case.search_sensors(parameter)
```

---

## 🏊‍♂️ **Connection Pool Details**

### **Pool Configuration** (Same for both patterns)

```python
pool_size=10          # 🔗 Base connections (always open)
max_overflow=20       # 🔗 Additional connections when pool exhausted
pool_timeout=30       # ⏱️ Wait time when pool is full
pool_recycle=3600     # 🔄 Refresh connections every hour
pool_pre_ping=True    # 🏥 Health check connections before use
```

### **Current Pool Usage** (from your investigation)

```bash
# Connection Distribution by Service:
172.19.0.9  (normalizer)     = 5 connections (1 long-lived + pool overhead)
172.19.0.14 (python-backend) = 1 connection (per-request sessions)
# Total: 6 active connections to PostgreSQL
```

---

## 🔀 **Session Lifecycle Comparison**

| Aspect                 | Data Pipeline Pattern             | Backend Pattern             |
| ---------------------- | --------------------------------- | --------------------------- |
| **Session Duration**   | Hours/Days (service lifetime)     | Seconds (request lifetime)  |
| **Session Count**      | 1 per service                     | 1 per HTTP request          |
| **Transaction Scope**  | Manual control                    | Auto-commit/rollback        |
| **Repository Sharing** | ✅ All repositories share session | ❌ New session per use case |
| **Connection Usage**   | 1 connection per service          | Multiple connections (pool) |
| **Error Handling**     | Service-level                     | Request-level               |

---

## 🔍 **Repository Usage Patterns**

### **Data Pipeline Repositories** (normalizer/service.py)

```python
# ALL REPOSITORIES USE THE SAME SESSION
async def _process_message(self, raw_msg_record):
    # Same session for all database operations in this message:
    device = await self.sensor_repository.find_one_by(parameter=device_id)         # 🔐 Session 1
    datatypes = await self.datatype_repository.get_sensor_datatypes_ordered()      # 🔐 Session 1
    hardware = await self.hardware_repository.get_hardware_by_sensor_parameter()   # 🔐 Session 1
    alarms = await self.alarm_repository.find_by(sensor_id=device.id)             # 🔐 Session 1

    # All queries happen within the same transaction!
```

### **Backend Repositories** (via use cases)

```python
# EACH USE CASE GETS ITS OWN SESSION
async def search_sensors(self, criteria):
    # New session for this use case only:
    async with get_session() as session:           # 🔐 New Session A
        sensor_repo = SQLSensorRepository(session)
        return await sensor_repo.search(criteria)  # 🔐 Uses Session A
    # Session A closed and returned to pool

# If same request needs another use case:
async def get_sensor_details(self, sensor_id):
    async with get_session() as session:           # 🔐 New Session B
        detail_repo = SQLSensorRepository(session)
        return await detail_repo.get(sensor_id)    # 🔐 Uses Session B
    # Session B closed and returned to pool
```

---

## ⚙️ **Connection Pool Monitoring**

### **Pool Status Queries**

```sql
-- Current active connections
SELECT
    client_addr,
    state,
    application_name,
    query_start,
    state_change
FROM pg_stat_activity
WHERE datname = 'preservarium'
ORDER BY backend_start;

-- Pool utilization
SELECT
    COUNT(*) as total_connections,
    COUNT(CASE WHEN state = 'active' THEN 1 END) as active_queries,
    COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_in_pool
FROM pg_stat_activity
WHERE datname = 'preservarium';
```

### **Pool Efficiency Metrics**

- **Data Pipeline**: Very efficient - 1 connection per service
- **Backend**: Moderate efficiency - connections shared across requests
- **Total Pool Usage**: ~6/30 connections (20% utilization)

---

## 🚨 **Connection Leak Prevention**

### **Data Pipeline Protection** (Long-lived sessions)

```python
# Context manager ensures cleanup
async with manage_database() as db_session:
    try:
        # Service runs with session
        await service.run()
    finally:
        await db_session.close()  # Always closes on service shutdown
```

### **Backend Protection** (Per-request sessions)

```python
async def get_session():
    session = async_session_factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()  # Rollback on any error
        raise
    finally:
        await session.close()     # Always returned to pool
```

---

## 💡 **Key Insights**

1. **Two Pools, Same Config**: Both patterns use the same connection pool configuration
2. **Different Lifecycles**: Pipeline services hold long-lived sessions; backend creates per-request sessions
3. **Efficient Usage**: Pipeline services are very connection-efficient (1 per service)
4. **Error Isolation**: Backend pattern isolates errors per request; pipeline pattern requires careful error handling
5. **Transaction Control**: Pipeline has manual transaction control; backend auto-manages transactions

The **timeout configurations you implemented earlier** protect both patterns from stuck connections!

---

## 🎯 **Next Steps for Optimization**

1. **Monitor pool utilization** - Current usage is low (6/30 connections)
2. **Consider connection sharing** for pipeline services with high message volumes
3. **Implement read/write splitting** for better performance
4. **Add connection pool health monitoring** to detect issues early

This architecture gives you both flexibility (backend) and efficiency (pipeline) while maintaining proper resource management across your entire system!
