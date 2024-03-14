CREATE TABLE IF NOT EXISTS stg.api_deliveries (
    object_id      varchar PRIMARY KEY,
	object_value   text NOT NULL,
	delivery_ts    timestamp NOT NULL
);