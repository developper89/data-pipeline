# app/repositories/alarm_repository.py
import logging
from typing import List, Optional

import asyncpg

logger = logging.getLogger(__name__)


class AlarmRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    async def update_alarms(self, fields: dict[str, List[dict[str, any]]]) -> None:
        query = """
        WITH input_data AS (
            SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
                alarm_id UUID,
                alert_id UUID,
                name TEXT,
                message TEXT,
                treated BOOLEAN,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                error_value FLOAT,
                updated BOOLEAN
            )
        ),
        insert_new_alerts AS (
            INSERT INTO alerts (name, message, treated, alarm_id, start_date, error_value)
            SELECT 
                name, message, treated, alarm_id, start_date, error_value
            FROM input_data
            WHERE alert_id IS NULL
            RETURNING id
        ),
        update_existing_alerts AS (
            UPDATE alerts
            SET 
                treated = input_data.treated,
                end_date = input_data.end_date
            FROM input_data
            WHERE 
                alerts.id = input_data.alert_id
                AND input_data.treated = true
                AND input_data.end_date IS NOT NULL
                AND input_data.updated = true
            RETURNING alerts.id
        )
        SELECT 
            COALESCE((SELECT COUNT(*) FROM insert_new_alerts), 0) AS inserted,
            COALESCE((SELECT COUNT(*) FROM update_existing_alerts), 0) AS updated
        """

        try:
            async with self.db_pool.acquire() as conn:
                flattened_data = []
                for _, alarms in fields.items():
                    for alarm in alarms:
                        for alert in alarm["alerts"]:
                            flattened_data.append(
                                {
                                    "alarm_id": alarm["id"],
                                    "alert_id": alert.get("id"),
                                    "name": alert.get("name"),
                                    "message": alert.get("message"),
                                    "treated": alert.get("treated"),
                                    "start_date": alert.get("start_date"),
                                    "end_date": alert.get("end_date"),
                                    "error_value": alert.get("error_value"),
                                    "updated": alert.get("updated", False),
                                }
                            )

                result = await conn.fetchrow(query, flattened_data)
                logger.info(
                    f"Inserted {result['inserted']} new alerts and updated {result['updated']} existing alerts"
                )
                return result
        except Exception as e:
            logger.error(f"Error updating alarms: {e}")
            raise

    async def get_active_alarms_for_sensor(self, sensor_parameter: str) -> List[dict]:
        query = """
        SELECT a.id, a.name, a.description, a.level, a.greaterthan, a.threshold, 
               a.field_name, a.datatype_id, d.name as datatype_name, d.unit
        FROM alarms a
        JOIN datatypes d ON a.datatype_id = d.id
        JOIN sensors s ON a.sensor_id = s.id
        WHERE s.parameter = $1 AND a.active = TRUE
        """
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, sensor_parameter)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(
                f"Error fetching active alarms for sensor parameter {sensor_parameter}: {e}"
            )
            return []
