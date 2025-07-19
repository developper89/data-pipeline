# Database Session Error Handling - Clean Architecture Plan

## 🚨 **Current Problem**

**Issue**: `PendingRollbackError: Can't reconnect until invalid transaction is rolled back`

**Root Cause**: The normalizer service uses a **single long-lived database session** shared across all repositories. When any database operation fails, it leaves the entire session in an invalid state, making all subsequent database operations fail.

**Current Architecture Problems**:

- ❌ Single session shared across multiple repositories
- ❌ No session error recovery mechanism
- ❌ Database errors cascade and break entire service
- ❌ Session state management mixed with business logic

---

## 🏗️ **Proposed Solution Architecture**

### **Option 1: Repository-Level Session Recovery (RECOMMENDED)**

**Concept**: Each repository handles its own session errors and recovery transparently.

#### **Implementation Approach**:

1. **Enhanced Base Repository**

   - Add session health checking before operations
   - Implement automatic rollback and retry on `PendingRollbackError`
   - Isolate session errors from business logic

2. **Session Recovery Pattern**

   ```python
   async def execute_with_recovery(self, operation):
       try:
           return await operation()
       except PendingRollbackError:
           await self.session.rollback()
           return await operation()  # Retry once
   ```

3. **Transparent Error Handling**
   - Business logic remains clean (no SQLAlchemy imports)
   - Repositories handle all database-level concerns
   - Automatic recovery without service interruption

#### **Files to Modify**:

- `packages/preservarium-sdk/preservarium_sdk/infrastructure/sql_repository/sql_base_repository.py`
- No changes to `normalizer/service.py` (clean separation)

---

### **Option 2: Session-Per-Operation Pattern**

**Concept**: Create fresh database sessions for each operation instead of long-lived sessions.

#### **Implementation Approach**:

1. **Database Session Factory**

   ```python
   async def with_session(operation):
       async with async_session_factory() as session:
           try:
               result = await operation(session)
               await session.commit()
               return result
           except Exception:
               await session.rollback()
               raise
   ```

2. **Repository Pattern Update**
   - Remove session sharing between repositories
   - Each operation gets fresh session
   - Automatic cleanup and error isolation

#### **Files to Modify**:

- `shared/db/database.py` - Add session factory helpers
- `normalizer/main.py` - Update session management pattern
- Repository initialization pattern

---

### **Option 3: Database Manager with Health Monitoring**

**Concept**: Add session health monitoring and automatic recovery at the database management level.

#### **Implementation Approach**:

1. **Enhanced Database Manager**

   ```python
   class DatabaseManager:
       async def ensure_healthy_session(self, session):
           if self.is_session_invalid(session):
               await session.rollback()
               logger.info("Session recovered from invalid state")
   ```

2. **Session Health Checks**
   - Check session state before operations
   - Automatic recovery when needed
   - Centralized session management

#### **Files to Modify**:

- `shared/db/database.py` - Add session health monitoring
- `normalizer/main.py` - Integrate health checking

---

## 🎯 **RECOMMENDED SOLUTION: Option 1 - Repository-Level Recovery**

### **Why This is the Best Approach**:

✅ **Clean Architecture** - Database concerns stay in repository layer  
✅ **Transparent Recovery** - Business logic remains unaware of database issues  
✅ **Minimal Changes** - Only modify base repository, not business logic  
✅ **Testable** - Easy to unit test session recovery logic  
✅ **Scalable** - Works for all repositories automatically

### **Implementation Plan**:

#### **Phase 1: Enhanced Base Repository**

1. **Add Session Recovery Methods**

   ```python
   async def _recover_session_if_needed(self):
       """Check and recover from invalid session state"""

   async def _execute_with_recovery(self, operation):
       """Execute database operation with automatic recovery"""
   ```

2. **Update Core Repository Methods**
   - Wrap all database operations (`find_one_by`, `create`, `update`, etc.)
   - Add recovery logic before each operation
   - Log recovery attempts for monitoring

#### **Phase 2: Repository Method Enhancement**

1. **Modify Base Methods**

   ```python
   async def find_one_by(self, **kwargs):
       return await self._execute_with_recovery(
           lambda: self._do_find_one_by(**kwargs)
       )
   ```

2. **Transparent Error Handling**
   - Catch `PendingRollbackError` at repository level
   - Automatic rollback and retry
   - No changes to service layer

#### **Phase 3: Monitoring and Logging**

1. **Session Health Metrics**

   - Count recovery attempts
   - Log session error patterns
   - Monitor recovery success rate

2. **Error Reporting**
   - Distinguish between recoverable and fatal errors
   - Provide meaningful error context
   - Maintain service reliability

---

## 📋 **Implementation Steps**

### **Step 1: Base Repository Enhancement**

- **File**: `sql_base_repository.py`
- **Action**: Add session recovery methods
- **Testing**: Unit tests for recovery logic

### **Step 2: Method Wrapping**

- **File**: `sql_base_repository.py`
- **Action**: Wrap database operations with recovery
- **Testing**: Integration tests with intentional session errors

### **Step 3: Service Integration**

- **File**: No changes to `normalizer/service.py`
- **Action**: Repositories handle errors transparently
- **Testing**: End-to-end testing with error injection

### **Step 4: Monitoring**

- **File**: Add session health logging
- **Action**: Monitor recovery patterns
- **Testing**: Verify error reporting and metrics

---

## ✅ **Expected Benefits**

1. **Service Reliability**

   - No more cascading database failures
   - Automatic recovery from transient errors
   - Service continues operating during database issues

2. **Clean Architecture**

   - Business logic stays focused on business concerns
   - Database concerns isolated in repository layer
   - Clear separation of responsibilities

3. **Maintainability**

   - Single point of database error handling
   - Easy to add monitoring and metrics
   - Consistent error handling across all repositories

4. **Performance**
   - Minimal overhead (only on errors)
   - No unnecessary session creation
   - Efficient recovery mechanism

---

## 🔄 **Rollback Plan**

If issues arise:

1. **Repository changes are isolated** - can be reverted easily
2. **No business logic changes** - service layer remains unchanged
3. **Backward compatible** - existing functionality preserved
4. **Gradual rollout** - can be applied per repository type

---

## 🧪 **Testing Strategy**

1. **Unit Tests**

   - Test session recovery logic in isolation
   - Mock database errors and verify recovery
   - Test retry limits and failure modes

2. **Integration Tests**

   - Test with real database connection issues
   - Verify end-to-end error recovery
   - Test concurrent operations during recovery

3. **Load Testing**
   - Verify performance impact is minimal
   - Test recovery under load
   - Monitor session pool behavior

---

**🎯 This plan provides a clean, architectural solution that fixes the database session errors without polluting the business logic layer.**
