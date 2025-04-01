# shared/db/database.py
import logging
from typing import AsyncGenerator
import asyncpg
import os

# Get the DATABASE_URL from environment and fix format if needed
raw_database_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/iot_config")
# Remove the +asyncpg suffix if present (SQLAlchemy format)


logger = logging.getLogger(__name__)

async def init_db() -> asyncpg.Pool:
    try:
        pool = await asyncpg.create_pool(str(raw_database_url))
        logger.info(f"Database connection pool created successfully for {raw_database_url.split('@')[1] if '@' in raw_database_url else raw_database_url}")
        return pool
    except Exception as e:
        logger.error(f"Failed to create database connection pool: {e}")
        raise


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await init_db()
    async with pool.acquire() as conn:
        yield conn
    await pool.close()