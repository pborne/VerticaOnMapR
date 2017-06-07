CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.shipping_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /shipping_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.shipping_dimension (
    shipping_key INT -- Scala Type: IntegerType
   ,ship_type VARCHAR(30) -- Scala Type: StringType
   ,ship_mode VARCHAR(10) -- Scala Type: StringType
   ,ship_carrier VARCHAR(20) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.shipping_dimension');
SELECT ANALYZE_STATISTICS('parquet.shipping_dimension');

SELECT count(*) FROM parquet.shipping_dimension;
SELECT * FROM parquet.shipping_dimension LIMIT 2;
