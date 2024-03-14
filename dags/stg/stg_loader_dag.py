import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.couriers_loader import CourierLoader
from examples.stg.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id = 'sprint5_project_api_to_stg',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_api_to_stg():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="couriers_load")
    def l_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="deliveries_load")
    def l_deliveries():
        deliv_loader = DeliveryLoader(origin_pg_connect, dwh_pg_connect, log)
        deliv_loader.load_deliveries()

    # Инициализируем объявленные таски.
    couriers_to_stg = l_couriers()
    deliveries_to_stg = l_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    couriers_to_stg # type: ignore
    deliveries_to_stg

stg_api_dag = sprint5_project_api_to_stg()
