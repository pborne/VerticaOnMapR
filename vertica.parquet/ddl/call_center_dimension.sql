CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.call_center_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /call_center_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.call_center_dimension (
    call_center_key INT -- Scala Type: IntegerType
   ,cc_closed_date DATE -- Scala Type: DateType
   ,cc_open_date DATE -- Scala Type: DateType
   ,cc_name VARCHAR(50) -- Scala Type: StringType
   ,cc_class VARCHAR(50) -- Scala Type: StringType
   ,cc_employees INT -- Scala Type: IntegerType
   ,cc_hours VARCHAR(20) -- Scala Type: StringType
   ,cc_manager VARCHAR(40) -- Scala Type: StringType
   ,cc_address VARCHAR(256) -- Scala Type: StringType
   ,cc_city VARCHAR(64) -- Scala Type: StringType
   ,cc_state VARCHAR(2) -- Scala Type: StringType
   ,cc_region VARCHAR(64) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.call_center_dimension');
SELECT ANALYZE_STATISTICS('parquet.call_center_dimension');

SELECT count(*) FROM parquet.call_center_dimension;
SELECT * FROM parquet.call_center_dimension LIMIT 2;
