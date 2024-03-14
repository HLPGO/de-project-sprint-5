import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
# from config_const import ConfigConst
from lib import ConnectionBuilder

from examples.dds.dds_settings_repository import DdsEtlSettingsRepository
#from examples.cdm.cdm_loader import LedgerCdmRepository
from examples.cdm.cdm_loader import LedgerLoader
from examples.cdm.schema_cdm import SchemaCdm


log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm_loader',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = DdsEtlSettingsRepository()
    
    @task(task_id="schema_init")
    def schema_init(ds=None, **kwargs):
        rest_loader = SchemaCdm(dwh_pg_connect)
        rest_loader.init_schema()    

    @task(task_id="cdm_loader")
    def cdm_loader(ds=None, **kwargs):
        reloader = LedgerLoader(dwh_pg_connect)
        reloader.reload_cdm

    cdm = cdm_loader()
    init = schema_init()

    init >> cdm
