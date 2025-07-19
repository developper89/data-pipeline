# Database Connection Idle Transaction Fixes

## 🚨 **Problem Summary**

**Issue**: PostgreSQL connection stuck in "idle in transaction" state for 6+ hours from the `python-backend` service.

**Root Cause**: Database session started transaction but never committed/rolled back due to:

- Client disconnection mid-request
- Unhandled exception during request processing
- Network interruption or timeout
- Incomplete error handling in session management

**Affected Connection**:

- **Service**: `python-backend` (172.19.0.14)
- **Query**: `SELECT sensors.* FROM sensors WHERE sensors.parameter = $1::VARCHAR`
- **Endpoint**: `GET /api/v1/sensors/search/?parameter={device_id}`
- **Duration**: 6+ hours stuck

---

## 🔧 **Recommended Fixes**

### **1. Immediate Fix - Restart Backend Service**

**Priority**: 🔴 **URGENT** - Execute immediately

```bash
# Restart the problematic service to clear stuck connection
cd ../../service-router
docker-compose restart python-backend
```

**Expected Result**: Clears the stuck connection and frees the database resource.

---

### **2. Add PostgreSQL Connection Timeouts**

**Priority**: 🟡 **HIGH** - Prevents future stuck connections

**File**: `packages/preservarium-sdk/preservarium_sdk/core/config.py`

**Changes**:

```python
class DatabaseSettings(BaseSettings):
    # ... existing settings ...

    # Add transaction timeout settings
    statement_timeout: str = "30s"                    # Query execution timeout
    idle_in_transaction_session_timeout: str = "60s"  # Idle transaction timeout
    lock_timeout: str = "30s"                        # Lock acquisition timeout

    # Connection tuning (update existing)
    connect_timeout: int = 30      # Reduce from 60 to 30 seconds
    command_timeout: int = 30      # Reduce from 60 to 30 seconds
```

**Benefit**: Automatically terminates stuck connections and prevents resource leaks.

---

### **3. Improve FastAPI Session Management**

**Priority**: 🟡 **HIGH** - Better error handling for client disconnections

**File**: `backend/app/infrastructure/database/database.py`

**Changes**:

```python
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Enhanced FastAPI dependency with better error handling"""
    session = async_session_factory()
    session_id = id(session)
    logger.debug(f"Created database session {session_id}")

    try:
        yield session
        await session.commit()
        logger.debug(f"Successfully committed session {session_id}")
    except asyncio.CancelledError:
        # Handle client disconnections gracefully
        await session.rollback()
        logger.warning(f"Request cancelled - rolled back session {session_id}")
        raise
    except Exception as e:
        await session.rollback()
        logger.error(f"Database session {session_id} error: {str(e)}")
        raise
    finally:
        try:
            await session.close()
            logger.debug(f"Closed database session {session_id}")
        except Exception as e:
            logger.error(f"Error closing session {session_id}: {e}")
```

**Benefit**: Proper handling of client disconnections and request cancellations.

---

### **4. Add Connection Pool Health Monitoring**

**Priority**: 🟢 **MEDIUM** - Proactive monitoring

**File**: `backend/app/infrastructure/database/pool_monitor.py`

**Enhancement**: Add periodic health checks for stuck connections

```python
async def check_for_stuck_connections(self) -> Dict[str, Any]:
    """Check for connections stuck in idle in transaction state"""
    try:
        async with self.engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT
                    client_addr,
                    state,
                    query_start,
                    state_change,
                    EXTRACT(EPOCH FROM (NOW() - state_change))/60 as minutes_stuck
                FROM pg_stat_activity
                WHERE datname = 'preservarium'
                  AND state = 'idle in transaction'
                  AND state_change < NOW() - INTERVAL '5 minutes'
            """))

            stuck_connections = result.fetchall()
            if stuck_connections:
                logger.warning(f"Found {len(stuck_connections)} stuck connections")
                for conn in stuck_connections:
                    logger.warning(f"Stuck connection: {conn.client_addr} for {conn.minutes_stuck:.1f} minutes")

            return {
                "stuck_connections": len(stuck_connections),
                "details": [dict(conn._mapping) for conn in stuck_connections]
            }
    except Exception as e:
        logger.error(f"Error checking stuck connections: {e}")
        return {"error": str(e)}
```

**Benefit**: Early detection and alerting for future connection issues.

---

### **5. Add Request Timeout Middleware**

**Priority**: 🟢 **MEDIUM** - Prevent long-running requests

**File**: `backend/app/infrastructure/middleware/timeout_middleware.py` (new file)

```python
import asyncio
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

class RequestTimeoutMiddleware(BaseHTTPMiddleware):
    """Middleware to enforce request timeouts"""

    def __init__(self, app, timeout: float = 30.0):
        super().__init__(app)
        self.timeout = timeout

    async def dispatch(self, request: Request, call_next):
        try:
            return await asyncio.wait_for(call_next(request), timeout=self.timeout)
        except asyncio.TimeoutError:
            return JSONResponse(
                status_code=504,
                content={"detail": f"Request timeout after {self.timeout}s"}
            )
```

**Benefit**: Prevents runaway requests from holding database connections indefinitely.

---

### **6. Database Connection Pool Configuration**

**Priority**: 🟢 **LOW** - Optimization

**File**: `backend/app/core/database_config.py`

**Enhancement**: Optimize pool settings for better resource management

```python
class DatabasePoolConfig:
    # Existing settings are good, but add these monitoring settings
    pool_logging: bool = True          # Enable pool logging
    pool_echo: bool = False           # Keep disabled for performance
    pool_reset_on_return: str = "commit"  # Ensure clean connections
```

**Benefit**: Better connection lifecycle management and monitoring.

---

## 📋 **Implementation Plan**

### **Phase 1: Immediate (Execute Now)**

1. ✅ **Restart python-backend service** - Clear current stuck connection

### **Phase 2: Short-term (This Session)**

2. 🔧 **Add PostgreSQL timeouts** - Prevent future stuck connections
3. 🔧 **Improve session management** - Better error handling
4. 🔧 **Add connection monitoring** - Detect issues early

### **Phase 3: Medium-term (Next Session)**

5. 🔧 **Add request timeout middleware** - Prevent long-running requests
6. 🔧 **Optimize pool configuration** - Fine-tune connection management

---

## 📊 **Expected Results**

| Fix                   | Impact                                               | Timeline   |
| --------------------- | ---------------------------------------------------- | ---------- |
| Restart Service       | ✅ **Immediate** - Clears stuck connection           | 30 seconds |
| Add Timeouts          | 🛡️ **Prevention** - Auto-kills stuck connections     | Ongoing    |
| Better Error Handling | 🎯 **Reliability** - Graceful disconnection handling | Ongoing    |
| Health Monitoring     | 📊 **Visibility** - Early detection of issues        | Ongoing    |

---

## 🔍 **Verification Steps**

After implementing fixes, verify success with:

```sql
-- Check for stuck connections
SELECT
    client_addr,
    state,
    query_start,
    state_change,
    EXTRACT(EPOCH FROM (NOW() - state_change))/60 as minutes_in_state
FROM pg_stat_activity
WHERE datname = 'preservarium'
  AND state = 'idle in transaction'
ORDER BY state_change;
```

**Success Criteria**: No connections stuck in "idle in transaction" for more than 2 minutes.

---

## 🚀 **Ready for Implementation**

Please review this document and confirm:

1. **Priority order** of fixes
2. **Implementation approach**
3. **Any additional considerations** for your environment

Once approved, I will implement these fixes systematically, starting with the immediate restart and progressing through the phases.
