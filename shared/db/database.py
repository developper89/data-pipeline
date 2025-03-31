# shared/db/database.py
import logging
from typing import AsyncGenerator
import asyncpg
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/iot_config")
logger = logging.getLogger(__name__)

async def init_db() -> asyncpg.Pool:
    try:
        pool = await asyncpg.create_pool(str(DATABASE_URL))
        logger.info("Database connection pool created successfully")
        return pool
    except Exception as e:
        logger.error(f"Failed to create database connection pool: {e}")
        raise


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await init_db()
    async with pool.acquire() as conn:
        yield conn
    await pool.close()