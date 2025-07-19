# shared/db/database.py
import asyncio
import logging
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError, DisconnectionError, TimeoutError

logger = logging.getLogger(__name__)

# Lightweight database configuration for pipeline services
class PipelineDBConfig:
    """Lightweight database configuration for data pipeline services."""
    
    def __init__(self):
        # Get DATABASE_URL from environment
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable must be set")
        
        # Database connection settings with optimized timeouts
        self.echo = os.getenv("DB_ECHO", "false").lower() == "true"
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "20"))
        self.pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "10"))
        self.pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "3600"))
        self.pool_pre_ping = os.getenv("DB_POOL_PRE_PING", "true").lower() == "true"
        
        # Optimized timeout settings (matching SDK configuration)
        self.command_timeout = int(os.getenv("DB_COMMAND_TIMEOUT", "30"))
        self.statement_timeout = os.getenv("DB_STATEMENT_TIMEOUT", "30s")
        self.idle_in_transaction_session_timeout = os.getenv("DB_IDLE_IN_TRANSACTION_TIMEOUT", "60s")
        self.lock_timeout = os.getenv("DB_LOCK_TIMEOUT", "30s")

# Initialize database configuration
db_config = PipelineDBConfig()

# Create async engine with timeout configurations
async_engine = create_async_engine(
    db_config.database_url,
    echo=db_config.echo,
    pool_size=db_config.pool_size,
    max_overflow=db_config.max_overflow,
    pool_timeout=db_config.pool_timeout,
    pool_recycle=db_config.pool_recycle,
    pool_pre_ping=db_config.pool_pre_ping,
    connect_args={
        "command_timeout": db_config.command_timeout,
        "server_settings": {
            "jit": "off",
            "statement_timeout": db_config.statement_timeout,
            "idle_in_transaction_session_timeout": db_config.idle_in_transaction_session_timeout,
            "lock_timeout": db_config.lock_timeout,
        },
    }
)

# Create async session factory
async_session_factory = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

class DatabaseSessionManager:
    """
    Enhanced database session manager with retry logic for long-running pipeline services.
    Handles PendingRollbackError and other database session issues automatically.
    """
    
    def __init__(self, session_factory, retry_attempts: int = 3, retry_delay: float = 1.0):
        self.session_factory = session_factory
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self._current_session = None
        
    async def get_session(self) -> AsyncSession:
        """Get or create a database session with recovery capabilities."""
        if self._current_session is None:
            self._current_session = await self._create_session_with_retry()
        return self._current_session
        
    async def _create_session_with_retry(self) -> AsyncSession:
        """Create session with retry logic on connection failures."""
        last_exception = None
        
        for attempt in range(self.retry_attempts):
            try:
                session = self.session_factory()
                # Test the session
                async with session as test_session:
                    await test_session.connection()
                logger.info(f"Pipeline database session created successfully (attempt {attempt + 1})")
                return session
                
            except (DisconnectionError, TimeoutError, PendingRollbackError) as e:
                last_exception = e
                logger.warning(f"Pipeline database connection attempt {attempt + 1} failed: {e}")
                
                if attempt < self.retry_attempts - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.info(f"Retrying pipeline database connection in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    continue
                    
        logger.error(f"All pipeline database connection attempts failed. Last error: {last_exception}")
        raise last_exception
        
    async def recover_session(self) -> bool:
        """Attempt to recover the current session from PendingRollbackError and other issues."""
        if self._current_session is None:
            return False
            
        try:
            # Try to rollback any pending transactions (fixes PendingRollbackError)
            if self._current_session.in_transaction():
                await self._current_session.rollback()
                logger.info("Pipeline database session: Successfully rolled back pending transaction")
                
            # Test if the session is now healthy
            async with self._current_session as test_session:
                await test_session.connection()
                
            logger.info("Pipeline database session recovered successfully from error")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline database session recovery failed: {e}")
            # Close the bad session and mark for recreation
            try:
                await self._current_session.close()
            except:
                pass
            self._current_session = None
            return False
            
    async def close(self):
        """Close the current pipeline database session."""
        if self._current_session:
            try:
                await self._current_session.close()
                logger.info("Pipeline database session closed")
            except Exception as e:
                logger.error(f"Error closing pipeline database session: {e}")
            finally:
                self._current_session = None


# Create a global session manager for pipeline services
session_manager = DatabaseSessionManager(async_session_factory)
