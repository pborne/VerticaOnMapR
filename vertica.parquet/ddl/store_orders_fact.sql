CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.store_orders_fact;
\i set_paths.sql

\set parquet_file :parquet_path /store_orders_fact.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.store_orders_fact (
    product_key INT -- Scala Type: IntegerType
   ,product_version INT -- Scala Type: IntegerType
   ,store_key INT -- Scala Type: IntegerType
   ,vendor_key INT -- Scala Type: IntegerType
   ,employee_key INT -- Scala Type: IntegerType
   ,order_number INT -- Scala Type: IntegerType
   ,date_ordered DATE -- Scala Type: DateType
   ,date_shipped DATE -- Scala Type: DateType
   ,expected_delivery_date DATE -- Scala Type: DateType
   ,date_delivered DATE -- Scala Type: DateType
   ,quantity_ordered INT -- Scala Type: IntegerType
   ,quantity_delivered INT -- Scala Type: IntegerType
   ,shipper_name VARCHAR(32) -- Scala Type: StringType
   ,unit_price INT -- Scala Type: IntegerType
   ,shipping_cost INT -- Scala Type: IntegerType
   ,total_order_cost INT -- Scala Type: IntegerType
   ,quantity_in_stock INT -- Scala Type: IntegerType
   ,reorder_level INT -- Scala Type: IntegerType
   ,overstock_ceiling INT -- Scala Type: IntegerType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.store_orders_fact');
SELECT ANALYZE_STATISTICS('parquet.store_orders_fact');

SELECT count(*) FROM parquet.store_orders_fact;
SELECT * FROM parquet.store_orders_fact LIMIT 2;
