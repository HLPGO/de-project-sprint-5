import json
from typing import List, Optional
from datetime import datetime
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.cdm.cdm_settings_repository import CdmEtlSettingsRepository, EtlSetting

class LedgerLoader:
    WF_KEY = "cdm_dm_courier_ledger_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"


    def __init__(self, pg: PgConnect, settings_repository: CdmEtlSettingsRepository) -> None:
        self.dwh = pg

        self.cdm = LedgerCdmRepository()
        self.settings_repository = settings_repository

    def reload_ledger(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime(2024, 1, 1)})

            self.cdm.insert_ledger(conn)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = datetime.utcnow()
            self.settings_repository.save_setting(conn, wf_setting)


class LedgerCdmRepository:
    def insert_ledger(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM cdm.dm_courier_ledger;
                with agg_info as (
                select
                    dc.courier_id,
                    dc.courier_name,
                    dt."year",
                    dt."month",
                    COUNT(distinct do2.order_key) as orders_count,
                    SUM(dd.order_sum) as order_sum,
                    AVG(dd.rate) as rate_avg,
                    SUM(dd.order_sum) * 0.25 as order_processing_fee,
                    sum(dd.tip_sum) as courier_tips_sum
                from dds.dm_couriers dc
                left join dds.dm_deliveries dd
                    on dc.courier_id = dd.courier_id
                left join dds.dm_orders do2
                    on dd.order_id = do2.order_key
                left join dds.dm_timestamps dt
                    on do2.timestamp_id = dt.id
                where 1=1
                    and do2.order_status = 'CLOSED'
                group by
                    dc.courier_id,
                    dc.courier_name,
                    dt."year",
                    dt."month"
                ),
                courier_sum as (
                select
                    courier_id,
                    year,
                    month,
                    SUM(courier_order_sum) as courier_order_sum
                from (
                    select distinct
                        dd.order_id,
                        dd.courier_id,
                        dt."year",
                        dt."month",
                        dd.order_sum,
                        ag.rate_avg,
                        case
                        when ag.rate_avg < 4 then
                        case
                            when dd.order_sum * 0.05 < 100 then 100.0
                            else dd.order_sum * 0.05
                        end
                        when ag.rate_avg >= 4 and ag.rate_avg < 4.5 then
                        case
                            when dd.order_sum * 0.07 < 150 then 150
                            else dd.order_sum * 0.07
                        end
                        when ag.rate_avg >= 4.5 and ag.rate_avg < 4.9 then
                        case
                            when dd.order_sum * 0.08 < 175 then 175
                            else dd.order_sum * 0.08
                        end
                        when ag.rate_avg >= 4.9 then
                        case
                            when dd.order_sum * 0.10 < 200 then 200
                            else dd.order_sum * 0.10
                        end
                        else null
                    end as courier_order_sum
                    from dds.dm_deliveries dd
                    left join dds.dm_orders do2 
                        on dd.order_id = do2.order_key
                    left join dds.dm_timestamps dt
                        on do2.timestamp_id = dt.id
                    left join agg_info as ag
                        on dt.year = ag.year
                        and dt.month = ag.month
                    where 1=1
                        and do2.order_status = 'CLOSED'
                ) as q
                group by
                    courier_id,
                    year,
                    month
                )
                insert into cdm.dm_courier_ledger(
                    courier_id, courier_name, settlement_year, settlement_month,
                    orders_count, orders_total_sum, rate_avg, 
                    order_processing_fee, courier_order_sum, courier_tips_sum, 
                    courier_reward_sum
                )
                select
                    agg.courier_id,
                    agg.courier_name,
                    agg.year,
                    agg.month,
                    agg.orders_count,
                    agg.order_sum,
                    agg.rate_avg,
                    agg.order_processing_fee,
                    agg.courier_tips_sum,
                    cs.courier_order_sum,
                    cs.courier_order_sum + agg.courier_tips_sum * 0.95 as courier_reward_sum
                from agg_info as agg
                left join courier_sum as cs
                    on agg.courier_id = cs.courier_id
                    and agg.month = cs.month
                    and agg.year = cs.year
                """
            )
