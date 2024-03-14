CREATE TABLE IF NOT EXISTS stg.api_couriers (
    object_id      varchar PRIMARY KEY,
	object_value   text NOT NULL,
	update_ts      timestamp NOT NULL
);