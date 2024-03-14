CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id serial NOT NULL,
	object_id varchar(30) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id)
);