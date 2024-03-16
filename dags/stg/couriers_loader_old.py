import requests
import json
from logging import Logger
from typing import List
from datetime import datetime

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierObj(BaseModel):
    object_id: str
    object_value: str
    event_ts: datetime


class CouriersApiRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    # данные для прогрузки
    # def list_objects(self, events_threshold: int, limit: int):
    def list_objects(self):
        
        cohort = "21"
        # Variable.get("X-COHORT")
        nickname = "usofob"
        # Variable.get("X-NICKNAME")
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net" + "/couriers"
        # Variable.get("API-URL") + "/couriers"
        api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
        # Variable.get("X-API-KEY")

        headers = {
            "X-Nickname": nickname,
            "X-cohort": cohort,
            "X-API-KEY": api_key
            }

        params = {
            "sort_field": "_id"
        }

        try:
            response = requests.get(url=url, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for non-200 status codes
            return response.json()
        except:
            raise ValueError

class CourierDestRepository:

    def insert_object(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_couriers(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts
                        ;
                """,
                {
                    "object_id": courier["_id"],
                    "object_value": json.dumps(courier, ensure_ascii=False),
                    "update_ts": datetime.utcnow()
                },
            )


class CourierLoader:
    WF_KEY = "api_couriers_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_update_ts"
    # BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersApiRepository(pg_origin)
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            # last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_objects()
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.stg.insert_object(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = datetime.utcnow()
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
