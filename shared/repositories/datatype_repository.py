# app/repositories/datatype_repository.py
import logging
from typing import List, Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)


class DataTypeRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    async def get_sensor_datatypes_ordered(
        self, sensor_parameter: str
    ) -> List[dict[str, any]]:
        query = """
        SELECT 
            d.id, 
            d.name, 
            d.rangemin, 
            d.rangemax, 
            d.parsing_op, 
            d.unit
        FROM 
            datatypes d
        JOIN 
            hardware_datatype hd ON d.id = hd.datatype_id
        JOIN 
            hardwares h ON hd.hardware_id = h.id
        JOIN 
            sensors s ON h.id = s.hardware_id
        JOIN 
            sentype_hardware_datatype_association shda ON hd.id = shda.hardware_datatype_id
        WHERE 
            s.parameter = $1
        ORDER BY 
            shda.position;
        """
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, sensor_parameter)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching datatypes for sensor {sensor_parameter}: {e}")
            return []
