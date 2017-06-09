CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.warehouse_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /warehouse_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.warehouse_dimension (
    warehouse_key INT -- Scala Type: IntegerType
   ,warehouse_name VARCHAR(20) -- Scala Type: StringType
   ,warehouse_address VARCHAR(256) -- Scala Type: StringType
   ,warehouse_city VARCHAR(60) -- Scala Type: StringType
   ,warehouse_state VARCHAR(2) -- Scala Type: StringType
   ,warehouse_region VARCHAR(32) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.warehouse_dimension');
SELECT ANALYZE_STATISTICS('parquet.warehouse_dimension');

SELECT count(*) FROM parquet.warehouse_dimension;
SELECT * FROM parquet.warehouse_dimension LIMIT 2;
