# app/repositories/status_alert_repository.py
import logging
from datetime import datetime
from typing import List

import asyncpg

logger = logging.getLogger(__name__)


class StatusAlertsRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    async def update_status_alerts(
        self, sensor_parameter: str, status_alerts: List[dict]
    ):
        query = """
        WITH sensor AS (
            SELECT id FROM sensors WHERE parameter = $1
        ), insert_new_alerts AS (
            INSERT INTO status_alerts (name, bit, sensor_id, start_date, treated)
            SELECT 
                (SELECT unnest(enum_range(NULL::status_enum)))[($2::jsonb->>'bit')::int + 1] AS name,
                ($2::jsonb->>'bit')::int AS bit,
                (SELECT id FROM sensor) AS sensor_id,
                NOW() AS start_date,
                FALSE AS treated
            FROM jsonb_array_elements($2::jsonb) AS status_alert
            WHERE NOT EXISTS (
                SELECT 1 FROM status_alerts sa
                WHERE sa.sensor_id = (SELECT id FROM sensor)
                AND sa.bit = (status_alert->>'bit')::int
                AND sa.treated = FALSE
            )
            AND (status_alert->>'treated')::int = 0
            RETURNING id
        ), update_existing_alerts AS (
            UPDATE status_alerts sa
            SET treated = TRUE, end_date = NOW()
            WHERE sa.sensor_id = (SELECT id FROM sensor)
            AND sa.bit = (status_alert->>'bit')::int
            AND sa.treated = FALSE
            AND (status_alert->>'treated')::int = 1
            FROM jsonb_array_elements($2::jsonb) AS status_alert
            RETURNING sa.id
        )
        SELECT count(*) as total_updates
        FROM (
            SELECT id FROM insert_new_alerts
            UNION ALL
            SELECT id FROM update_existing_alerts
        ) AS combined_updates
        """
        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval(query, sensor_parameter, status_alerts)
                logger.info(
                    f"Updated {result} status alerts for sensor parameter {sensor_parameter}"
                )
                return result
        except Exception as e:
            logger.error(
                f"Error updating status alerts for sensor parameter {sensor_parameter}: {e}"
            )
            raise
