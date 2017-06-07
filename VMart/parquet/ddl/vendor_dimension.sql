CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.vendor_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /vendor_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.vendor_dimension (
    vendor_key INT -- Scala Type: IntegerType
   ,vendor_name VARCHAR(64) -- Scala Type: StringType
   ,vendor_address VARCHAR(64) -- Scala Type: StringType
   ,vendor_city VARCHAR(64) -- Scala Type: StringType
   ,vendor_state VARCHAR(2) -- Scala Type: StringType
   ,vendor_region VARCHAR(32) -- Scala Type: StringType
   ,deal_size INT -- Scala Type: IntegerType
   ,last_deal_update DATE -- Scala Type: DateType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.vendor_dimension');
SELECT ANALYZE_STATISTICS('parquet.vendor_dimension');

SELECT count(*) FROM parquet.vendor_dimension;
SELECT * FROM parquet.vendor_dimension LIMIT 2;
