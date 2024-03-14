### TLDR

Итого нам нужна таблица

Состав витрины:

| поле                 | описание                                                                                   | источник данных                                         |
| -------------------- | -------------------------------------------------------------------------------------------| --------------------------------------------------------|
| id                   | идентификатор записи.                                                                      | автогенерация                                           |
| courier_id           | ID курьера, которому перечисляем.                                                          | _id из get /couriers                                    |
| courier_name         | Ф. И. О. курьера.                                                                          | name из get /couriers                                   |
| settlement_year      | год отчёта.                                                                                | генерация                                               |
| settlement_month     | месяц отчёта, где 1 — январь и 12 — декабрь.                                               | генерация                                               |
| orders_count         | количество заказов за период (месяц).                                                      | countd _id из get /deliveries (+courier_id)             |
| orders_total_sum     | общая стоимость заказов.                                                                   | из orders + fct_product_sales                           |
| rate_avg             | средний рейтинг курьера по оценкам пользователей.                                          | avg rate из get /deliveries (+courier_id)               |
| order_processing_fee | сумма, удержанная компанией за обработку заказов, orders_total_sum * 0.25.                 | order_id из get /delivieries + order_id из              |
| courier_order_sum    | сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. (см. ниже).    | из orders + fct_product_sales + rate из get /deliveries |
| courier_tips_sum     | сумма, которую пользователи оставили курьеру в качестве чаевых.                            | tip_sum из get /delivieries (+courier_id)               |
| courier_reward_sum   | сумма, которую необходимо перечислить курьеру. courier_order_sum + courier_tips_sum * 0.95 | из orders + fct_product_sales                           |

Добавить таблицы в слой DDS курьеры и доставки

### 2.1. Задайте структуру витрины расчётов с курьерами

DDL-Запрос для cdm.dm_courier_ledger:
```
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id                   SERIAL,
    courier_id           varchar(255)   NOT NULL,
    courier_name         varchar(255)   NOT NULL,
    settlement_year      int            NOT NULL,
    settlement_month     INT            NOT NULL,
    orders_count         INT            NOT NULL,
    orders_total_sum     numeric(19, 5) NOT NULL,
    rate_avg             numeric(19, 5) NOT NULL,
    order_processing_fee numeric(19, 5) NOT NULL,
    courier_order_sum    numeric(19, 5) NOT NULL,
    courier_tips_sum     numeric(19, 5) NOT NULL,
    courier_reward_sum   numeric(19, 5) NOT NULL,
    CONSTRAINT settlement_year_check CHECK (settlement_year > 2020),
    CONSTRAINT settlement_month_check CHECK (settlement_month >= 1 AND settlement_month <= 12),
    CONSTRAINT orders_count_check CHECK (orders_count >= (0)::numeric),
    CONSTRAINT orders_total_sum_check CHECK (orders_total_sum >=(0)::numeric),
    CONSTRAINT rate_avg_check CHECK (rate_avg >=(0)::numeric),
    CONSTRAINT order_processing_fee_check CHECK (order_processing_fee >=(0)::numeric),
    CONSTRAINT courier_order_sum_check CHECK (courier_order_sum >=(0)::numeric),
    CONSTRAINT courier_tips_sum_check CHECK (courier_tips_sum >=(0)::numeric),
    CONSTRAINT courier_reward_sum_check CHECK (courier_reward_sum >=(0)::numeric)
)
```

### 2.2. Спроектируйте структуру DDS-слоя
Курьеры:
* dds.dm_couriers:

```
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id              SERIAL NOT NULL
    courier_id      varchar(255) NOT NULL,
    courier_name    varchar(255) NOT NULL,
)
```
* dds.dm_deliveries:

```
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id          SERIAL NOT NULL,
    delivery_id varchar(255) NOT NULL,
    order_id    varchar(255) NOT NULL,
    courier_id  varchar(255) NOT NULL,
    rate        int NOT NULL,
    order_sum   numeric(19, 5) NOT NULL,
    tip_sum     numeric(19, 5) NOT NULL,
    delivery_ts timestamp NOT NULL,
    CONSTRAINT  order_sum_check CHECK (order_sum >=(0)::numeric),
    CONSTRAINT  tip_sum CHECK (tip_sum >=(0)::numeric)
)
```

Свяжем таблицы dm_deliveries и dm_orders
```
ALTER TABLE dds.dm_deliveries
ADD CONSTRAINT order_id_fk FOREIGN KEY (order_id)
REFERENCES dds.dm_orders(order_key)
```

### 2.3. Спроектируйте структуру STG-слоя

stg.api_couriers
```
CREATE TABLE IF NOT EXISTS stg.api_couriers (
    object_id      varchar PRIMARY KEY,
	object_value   text NOT NULL,
	update_ts      timestamp NOT NULL
)
```
stg.api_deliveries

```
CREATE TABLE IF NOT EXISTS stg.api_deliveries (
    object_id      varchar PRIMARY KEY,
	object_value   text NOT NULL,
	delivery_ts    timestamp NOT NULL
)
```


