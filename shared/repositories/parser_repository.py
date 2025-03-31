# app/repositories/parser_repository.py

import logging
from typing import Any, Dict, Optional

import asyncpg

logger = logging.getLogger(__name__)

class DeviceNotFoundError(Exception):
    pass

class MultipleDevicesFoundError(Exception):
    pass

class ParserRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    async def get_parser_for_sensor(self, sensor_id: str) -> Optional[Dict[str, Any]]:
        query = """
        SELECT p.id, p.name, p.file_path
        FROM sensors s
        JOIN hardwares h ON s.hardware_id = h.id
        JOIN parsers p ON h.parser_id = p.id
        WHERE s.parameter = $1
        """

        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(query, sensor_id)
                if row:
                    return dict(row)
                else:
                    # logger.warning(f"No parser found for sensor_id: {sensor_id}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching parser for sensor {sensor_id}: {e}")
            return None
