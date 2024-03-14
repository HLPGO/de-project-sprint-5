import json
from typing import List, Optional
from datetime import datetime
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting


class DeliveryJsonObj(BaseModel):
    object_id: str
    object_value: str
    delivery_ts: datetime


class DeliveryDdsObj(BaseModel):
    delivery_id: str
    courier_id: str
    rate: int
    order_sum: float
    tip_sum: float
    delivery_ts: datetime
    order_id: str

class DeliveryRawRepository:
    def load_raw_deliverys(self, conn: Connection, last_loaded_delivery_ts: datetime) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        object_id,
                        object_value,
                        delivery_ts
                    FROM stg.api_deliveries
                    WHERE delivery_ts > %(last_loaded_delivery_ts)s
                """,
                {"last_loaded_delivery_ts": last_loaded_delivery_ts}
            )
            objs = cur.fetchall()
        return objs


class DeliveryDdsRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(
                        delivery_id,
                        courier_id,
                        rate,
                        order_sum,
                        tip_sum,
                        delivery_ts,
                        order_id
                    )
                    VALUES (
                        %(delivery_id)s,
                        %(courier_id)s,
                        %(rate)s,
                        %(order_sum)s,
                        %(tip_sum)s,
                        %(delivery_ts)s,
                        %(order_id)s
                    );
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "rate": delivery.rate,
                    "order_sum": delivery.order_sum,
                    "tip_sum": delivery.tip_sum,
                    "delivery_ts": delivery.delivery_ts,
                    "order_id": delivery.order_id
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryDdsObj]:
        with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        delivery_id,
                        courier_id,
                        rate,
                        order_sum,
                        tip_sum,
                        delivery_ts,
                        order_id
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj


class DeliveryLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow2"
    LAST_LOADED_ID_KEY = "last_loaded_delivery_ts"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = DeliveryRawRepository()
        self.dds = DeliveryDdsRepository()
        self.settings_repository = settings_repository

    def parse_deliveries(self, raws: List[DeliveryJsonObj]) -> List[DeliveryDdsObj]:
        res = []
        for r in raws:
            delivery_json = json.loads(r.object_value)
            t = DeliveryDdsObj(
                           delivery_id=delivery_json['delivery_id'],
                           courier_id=delivery_json['courier_id'],
                           rate=delivery_json['rate'],
                           order_sum=delivery_json['sum'],
                           tip_sum=delivery_json['tip_sum'],
                           delivery_ts=delivery_json['delivery_ts'],
                           order_id=delivery_json['order_id']
                           )

            res.append(t)
        return res

    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime(2024, 1, 1)})

            last_loaded_delivery_ts = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_deliverys(conn, last_loaded_delivery_ts)
            # load_queue.sort(key=lambda x: x.update_ts)
            deliverys_to_load = self.parse_deliveries(load_queue)
            for delivery in deliverys_to_load:
                existing = self.dds.get_delivery(conn, delivery.delivery_id)
                if not existing:
                    self.dds.insert_delivery(conn, delivery)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = delivery.delivery_ts
                self.settings_repository.save_setting(conn, wf_setting)
