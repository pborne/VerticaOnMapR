CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.inventory_fact;
\i set_paths.sql

\set parquet_file :parquet_path /inventory_fact.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.inventory_fact (
    date_key INT -- Scala Type: IntegerType
   ,product_key INT -- Scala Type: IntegerType
   ,product_version INT -- Scala Type: IntegerType
   ,warehouse_key INT -- Scala Type: IntegerType
   ,qty_in_stock INT -- Scala Type: IntegerType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.inventory_fact');
SELECT ANALYZE_STATISTICS('parquet.inventory_fact');

SELECT count(*) FROM parquet.inventory_fact;
SELECT * FROM parquet.inventory_fact LIMIT 2;
