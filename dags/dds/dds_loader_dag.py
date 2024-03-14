import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
#from config_const import ConfigConst
from lib import ConnectionBuilder

from examples.dds.dds_settings_repository import DdsEtlSettingsRepository
from examples.dds.courier_loader import CourierLoader
from examples.dds.delivery_loader import DeliveryLoader

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_stg_to_dds',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = DdsEtlSettingsRepository()
    
    @task(task_id="dm_couriers_load")
    def load_couriers(ds=None, **kwargs):
        couriers_loader = CourierLoader(dwh_pg_connect, settings_repository)
        couriers_loader.load_couriers()


    @task(task_id="dm_deliveries_load")
    def load_deliveries(ds=None, **kwargs):
        deliveries_loader = DeliveryLoader(dwh_pg_connect, settings_repository)
        deliveries_loader.load_deliveries()

    dm_couriers = load_couriers()
    dm_deliveries = load_deliveries()
    
    [dm_deliveries, dm_couriers]
  