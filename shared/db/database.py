# shared/db/database.py
import logging
from typing import AsyncGenerator
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

# Get the DATABASE_URL from environment
database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise ValueError("DATABASE_URL environment variable must be set")

# Create async engine with timeout configurations
async_engine = create_async_engine(
    str(database_url), 
    echo=False,
    connect_args={
        "command_timeout": 30,  # Query timeout
        "server_settings": {
            "statement_timeout": "30s",  # Statement timeout
            "idle_in_transaction_session_timeout": "60s",  # Auto-kill stuck transactions
            "lock_timeout": "30s",  # Lock timeout
        },
    }
)

# Create async session factory
async_session_factory = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def init_db() -> AsyncSession:
    """
    Initialize database connection and return a session.
    For long-running services that need to manage session lifecycle.
    
    Returns:
        AsyncSession: A database session for interacting with the database.
    """
    try:
        session = async_session_factory()
        # Test the connection
        async with session as test_session:
            await test_session.connection()
        logger.info(f"Database connection established successfully to {database_url.split('@')[1] if '@' in database_url else database_url}")
        return session
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise