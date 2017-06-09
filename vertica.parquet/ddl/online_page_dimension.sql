CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.online_page_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /online_page_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.online_page_dimension (
    online_page_key INT -- Scala Type: IntegerType
   ,start_date DATE -- Scala Type: DateType
   ,end_date DATE -- Scala Type: DateType
   ,page_number INT -- Scala Type: IntegerType
   ,page_description VARCHAR(100) -- Scala Type: StringType
   ,page_type VARCHAR(100) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.online_page_dimension');
SELECT ANALYZE_STATISTICS('parquet.online_page_dimension');

SELECT count(*) FROM parquet.online_page_dimension;
SELECT * FROM parquet.online_page_dimension LIMIT 2;
