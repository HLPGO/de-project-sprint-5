from lib import PgConnect


class SchemaCdm:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

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
);

CREATE TABLE if not exists cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar(30) NOT NULL,
	restaurant_name varchar(30) NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0 NOT NULL,
	restaurant_reward_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dm_settlement_report_id_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date))),
	CONSTRAINT dm_settlement_report_uniqe_columns UNIQUE (restaurant_id, settlement_date)
);

DO $do$ BEGIN IF EXISTS (
    SELECT
    FROM pg_catalog.pg_roles
    WHERE rolname = 'sp5_de_tester'
) THEN
GRANT SELECT ON all tables IN SCHEMA dds TO sp5_de_tester;
END IF;
END $do$;
"""
                )
