import json
from typing import List, Optional
from datetime import datetime
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.cdm.cdm_settings_repository import CdmEtlSettingsRepository, EtlSetting

class SrLoader:
    WF_KEY = "cdm_dm_settlement_report_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"

    # settings_repository: CdmEtlSettingsRepository
    def __init__(self, pg: PgConnect, settings_repository: CdmEtlSettingsRepository) -> None:
        self.dwh = pg

        self.cdm = SrCdmRepository()
        self.settings_repository = settings_repository

    def reload_sr(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime(2024, 1, 1)})

            self.cdm.insert_sr(conn)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = datetime.utcnow()
            self.settings_repository.save_setting(conn, wf_setting)
            
class SrCdmRepository:
    def insert_sr(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM cdm.dm_settlement_report;
                INSERT INTO cdm.dm_settlement_report (	restaurant_id, 
										restaurant_name, 
										settlement_date, 
										orders_count, 
										orders_total_sum, 
										orders_bonus_payment_sum, 
										orders_bonus_granted_sum, 
										order_processing_fee, 
										restaurant_reward_sum)
                WITH sales AS (
                    SELECT
                        dds.fct_product_sales.order_id AS order_id,
                        sum(dds.fct_product_sales.total_sum) AS ts,
                        sum(dds.fct_product_sales.bonus_payment) AS bp,
                        sum(dds.fct_product_sales.bonus_grant) AS bg
                    FROM dds.fct_product_sales
                    GROUP BY dds.fct_product_sales.order_id
                    ORDER BY dds.fct_product_sales.order_id
                    )
                SELECT 	dds.dm_restaurants.restaurant_id, 
                        dds.dm_restaurants.restaurant_name, 
                        dds.dm_timestamps."date" AS settlement_date,
                        count(sales.order_id) AS orders_count,
                        sum(sales.ts) AS orders_total_sum,
                        sum(sales.bp) AS orders_bonus_payment_sum,
                        sum(sales.bg) AS orders_bonus_granted_sum,
                        sum(sales.ts) * 0.25 AS order_processing_fee,
                        sum(sales.ts - sales.ts * 0.25 - sales.bp) AS restaurant_reward_sum
                FROM dds.dm_orders 
                LEFT JOIN dds.dm_restaurants ON dds.dm_orders.restaurant_id = dds.dm_restaurants.id
                LEFT JOIN dds.dm_timestamps ON dds.dm_orders.timestamp_id = dds.dm_timestamps.id
                LEFT JOIN sales ON sales.order_id = dds.dm_orders.id
                WHERE dds.dm_orders.order_status = 'CLOSED'
                GROUP BY dds.dm_restaurants.restaurant_id, dds.dm_restaurants.restaurant_name, settlement_date
                ON CONFLICT (restaurant_id, settlement_date)
                DO UPDATE SET 	restaurant_name = excluded.restaurant_name,
                                settlement_date = excluded.settlement_date, 
                                orders_count = EXCLUDED.orders_count, 
                                orders_total_sum = EXCLUDED.orders_total_sum, 
                                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum, 
                                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum, 
                                order_processing_fee = EXCLUDED.order_processing_fee, 
                                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """
            )

