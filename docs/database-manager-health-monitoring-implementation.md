# Option 3: Database Manager with Health Monitoring - Implementation Plan

## 🎯 **Concept Overview**

**Approach**: Add session health monitoring and automatic recovery at the database management level through a centralized `DatabaseManager` that monitors and maintains session health across the application.

**Philosophy**: Instead of handling errors reactively, proactively monitor session health and prevent errors before they occur.

---

## 🏗️ **Architecture Design**

### **Core Components**

1. **DatabaseManager Class**

   - Central session health monitoring
   - Automatic session recovery
   - Session state tracking
   - Health metrics collection

2. **Session Health Checker**

   - Detect invalid session states
   - Proactive session validation
   - Connection pool monitoring

3. **Recovery Orchestrator**
   - Coordinate session recovery across repositories
   - Manage recovery retries and fallbacks
   - Log recovery events and metrics

---

## 📋 **Detailed Implementation Steps**

### **Step 1: Create Enhanced Database Manager**

#### **1.1 DatabaseManager Class Structure**

**File**: `shared/db/database_manager.py` (NEW)

```python
class DatabaseManager:
    """
    Centralized database session manager with health monitoring and recovery.
    """

    def __init__(self, engine, session_factory):
        self.engine = engine
        self.session_factory = session_factory
        self.session_registry = {}  # Track active sessions
        self.health_metrics = {
            'recoveries': 0,
            'failed_recoveries': 0,
            'session_count': 0,
            'last_health_check': None
        }

    async def get_healthy_session(self, session_id: str = None) -> AsyncSession:
        """Get or create a healthy database session"""

    async def ensure_session_health(self, session: AsyncSession) -> bool:
        """Ensure session is in healthy state"""

    async def recover_session(self, session: AsyncSession) -> bool:
        """Recover a session from invalid state"""

    async def health_check_all_sessions(self) -> Dict[str, bool]:
        """Check health of all registered sessions"""

    def get_health_metrics(self) -> Dict[str, Any]:
        """Get current health metrics"""
```

#### **1.2 Session Health Detection Logic**

```python
async def is_session_healthy(self, session: AsyncSession) -> Tuple[bool, str]:
    """
    Check if session is in healthy state.
    Returns: (is_healthy, reason)
    """
    try:
        # Check if session has pending rollback
        if session.in_transaction() and session.get_transaction().is_active:
            # Try a simple SELECT 1 to test connection
            result = await session.execute(text("SELECT 1"))
            await result.fetchone()
            return True, "healthy"

        elif session.in_transaction():
            return False, "pending_transaction"

        else:
            return True, "no_active_transaction"

    except PendingRollbackError:
        return False, "pending_rollback_error"
    except Exception as e:
        return False, f"connection_error: {str(e)}"
```

#### **1.3 Automatic Session Recovery**

```python
async def recover_session(self, session: AsyncSession) -> bool:
    """
    Attempt to recover session from invalid state.
    Returns True if recovery successful.
    """
    try:
        # First, try to rollback any pending transactions
        if session.in_transaction():
            await session.rollback()
            logger.info("Session recovered: rolled back pending transaction")

        # Test the recovered session
        is_healthy, reason = await self.is_session_healthy(session)

        if is_healthy:
            self.health_metrics['recoveries'] += 1
            return True
        else:
            # If rollback didn't work, try closing and creating new session
            await session.close()
            logger.warning(f"Session recovery failed: {reason}, session closed")
            self.health_metrics['failed_recoveries'] += 1
            return False

    except Exception as e:
        logger.error(f"Session recovery failed with exception: {e}")
        self.health_metrics['failed_recoveries'] += 1
        return False
```

---

### **Step 2: Update Database Infrastructure**

#### **2.1 Enhanced database.py**

**File**: `shared/db/database.py`

```python
# Import the new DatabaseManager
from .database_manager import DatabaseManager

# Create database manager instance
database_manager = DatabaseManager(async_engine, async_session_factory)

async def get_managed_session(session_id: str = None) -> AsyncSession:
    """
    Get a health-monitored database session.
    """
    return await database_manager.get_healthy_session(session_id)

async def init_db() -> AsyncSession:
    """
    Initialize database with health monitoring.
    """
    try:
        # Get a managed session instead of raw session
        session = await get_managed_session("main_service_session")

        # Test the connection with health check
        is_healthy, reason = await database_manager.is_session_healthy(session)

        if is_healthy:
            logger.info(f"Database connection established successfully with health monitoring")
            return session
        else:
            logger.error(f"Database session unhealthy on init: {reason}")
            raise RuntimeError(f"Unhealthy database session: {reason}")

    except Exception as e:
        logger.error(f"Failed to create managed database connection: {e}")
        raise
```

#### **2.2 Session Context Manager Enhancement**

```python
@asynccontextmanager
async def managed_database_session(session_id: str = None):
    """
    Context manager for health-monitored database sessions.
    """
    session = None
    try:
        # Get managed session
        session = await database_manager.get_healthy_session(session_id)

        # Pre-operation health check
        is_healthy, reason = await database_manager.is_session_healthy(session)
        if not is_healthy:
            logger.warning(f"Session unhealthy before operation: {reason}")
            recovered = await database_manager.recover_session(session)
            if not recovered:
                raise RuntimeError(f"Could not recover unhealthy session: {reason}")

        yield session

        # Commit if everything went well
        if session.in_transaction():
            await session.commit()

    except Exception as e:
        # Handle errors with recovery attempt
        if session:
            try:
                is_healthy, reason = await database_manager.is_session_healthy(session)
                if not is_healthy:
                    await database_manager.recover_session(session)
                else:
                    await session.rollback()
            except Exception as recovery_error:
                logger.error(f"Error during session recovery: {recovery_error}")
        raise e
    finally:
        if session:
            # Final health check and cleanup
            try:
                await session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
```

---

### **Step 3: Integration with Normalizer Service**

#### **3.1 Update normalizer/main.py**

```python
@asynccontextmanager
async def manage_database() -> AsyncGenerator[AsyncSession, None]:
    """
    Enhanced async context manager for database session with health monitoring.
    """
    db_session = None
    try:
        logger.info("Initializing managed database connection...")

        # Use managed session instead of direct init_db
        db_session = await get_managed_session("normalizer_service")

        # Register session with health monitoring
        database_manager.register_session("normalizer_service", db_session)

        # Start background health monitoring
        health_monitor_task = asyncio.create_task(
            database_manager.continuous_health_monitoring("normalizer_service")
        )

        yield db_session

    finally:
        # Cleanup
        if health_monitor_task:
            health_monitor_task.cancel()

        if db_session:
            database_manager.unregister_session("normalizer_service")
            logger.info("Closing managed database connection...")
            await db_session.close()
```

#### **3.2 Repository Factory with Health Monitoring**

```python
async def create_repositories_with_health_monitoring(session: AsyncSession):
    """
    Create repositories with health-monitored session.
    """

    # Ensure session is healthy before creating repositories
    is_healthy, reason = await database_manager.is_session_healthy(session)
    if not is_healthy:
        logger.warning(f"Recovering session before repository creation: {reason}")
        recovered = await database_manager.recover_session(session)
        if not recovered:
            raise RuntimeError(f"Cannot create repositories with unhealthy session: {reason}")

    # Create repositories with monitored session
    return {
        'datatype_repository': SQLDatatypeRepository(session),
        'sensor_repository': SQLSensorRepository(session),
        'broker_repository': SQLBrokerRepository(session),
        'hardware_repository': SQLHardwareRepository(session),
        'alarm_repository': SQLAlarmRepository(session),
        'alert_repository': SQLAlertRepository(session),
        'user_repository': SQLUserRepository(session),
    }
```

---

### **Step 4: Background Health Monitoring**

#### **4.1 Continuous Health Monitoring**

```python
async def continuous_health_monitoring(self, session_id: str, interval: int = 30):
    """
    Continuously monitor session health in background.
    """
    while True:
        try:
            await asyncio.sleep(interval)

            session = self.session_registry.get(session_id)
            if session:
                is_healthy, reason = await self.is_session_healthy(session)

                if not is_healthy:
                    logger.warning(f"Health monitor detected unhealthy session {session_id}: {reason}")
                    recovered = await self.recover_session(session)

                    if recovered:
                        logger.info(f"Background health monitor recovered session {session_id}")
                    else:
                        logger.error(f"Background health monitor failed to recover session {session_id}")

                # Update health metrics
                self.health_metrics['last_health_check'] = datetime.now()

        except asyncio.CancelledError:
            logger.info(f"Health monitoring cancelled for session {session_id}")
            break
        except Exception as e:
            logger.error(f"Error in health monitoring for session {session_id}: {e}")
```

#### **4.2 Health Metrics Endpoint**

```python
def get_health_report(self) -> Dict[str, Any]:
    """
    Generate comprehensive health report.
    """
    active_sessions = len(self.session_registry)

    return {
        'timestamp': datetime.now().isoformat(),
        'active_sessions': active_sessions,
        'total_recoveries': self.health_metrics['recoveries'],
        'failed_recoveries': self.health_metrics['failed_recoveries'],
        'recovery_success_rate': (
            self.health_metrics['recoveries'] /
            max(1, self.health_metrics['recoveries'] + self.health_metrics['failed_recoveries'])
        ) * 100,
        'last_health_check': self.health_metrics['last_health_check'],
        'session_registry': list(self.session_registry.keys())
    }
```

---

### **Step 5: Integration Points**

#### **5.1 Files to Create**

- `shared/db/database_manager.py` - Core DatabaseManager class
- `shared/db/health_monitor.py` - Health monitoring utilities
- `shared/db/recovery_strategies.py` - Different recovery strategies

#### **5.2 Files to Modify**

- `shared/db/database.py` - Integration with DatabaseManager
- `normalizer/main.py` - Use managed sessions
- `normalizer/service.py` - NO CHANGES (clean separation)

#### **5.3 Configuration Options**

**Environment Variables**:

```bash
# Health monitoring settings
DB_HEALTH_CHECK_INTERVAL=30          # Background check interval (seconds)
DB_RECOVERY_MAX_ATTEMPTS=3           # Max recovery attempts
DB_HEALTH_CHECK_TIMEOUT=5            # Health check query timeout
DB_SESSION_MAX_IDLE_TIME=300         # Max idle time before health check
DB_ENABLE_BACKGROUND_MONITORING=true # Enable/disable background monitoring
```

---

### **Step 6: Testing Strategy**

#### **6.1 Unit Tests**

- Test session health detection accuracy
- Test recovery mechanisms
- Test health metrics collection

#### **6.2 Integration Tests**

- Test with intentional database connection issues
- Test background monitoring effectiveness
- Test recovery under load

#### **6.3 Monitoring Tests**

- Verify health metrics are accurate
- Test health report generation
- Test alert systems for recovery failures

---

## ✅ **Benefits of Option 3**

### **Advantages**:

1. **Proactive Monitoring** - Prevents errors before they occur
2. **Centralized Management** - Single point of session health control
3. **Rich Metrics** - Detailed health and recovery statistics
4. **Background Recovery** - Continuous session health maintenance
5. **Zero Business Logic Impact** - Service layer remains unchanged

### **Trade-offs**:

1. **Complexity** - More moving parts and monitoring overhead
2. **Resource Usage** - Background monitoring tasks consume resources
3. **Initial Setup** - More extensive setup compared to other options
4. **Debugging** - More complex debugging of health monitoring issues

---

## 🚀 **Implementation Timeline**

- **Week 1**: Create DatabaseManager and health detection logic
- **Week 2**: Integrate with existing database infrastructure
- **Week 3**: Add background monitoring and metrics
- **Week 4**: Testing and performance optimization

---

**🎯 This approach provides the most comprehensive solution with proactive session health management, but requires the most initial investment in infrastructure.**
