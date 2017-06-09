CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.store_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /store_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.store_dimension (
    store_key INT -- Scala Type: IntegerType
   ,store_name VARCHAR(64) -- Scala Type: StringType
   ,store_number INT -- Scala Type: IntegerType
   ,store_address VARCHAR(256) -- Scala Type: StringType
   ,store_city VARCHAR(64) -- Scala Type: StringType
   ,store_state VARCHAR(2) -- Scala Type: StringType
   ,store_region VARCHAR(64) -- Scala Type: StringType
   ,floor_plan_type VARCHAR(32) -- Scala Type: StringType
   ,photo_processing_type VARCHAR(32) -- Scala Type: StringType
   ,financial_service_type VARCHAR(32) -- Scala Type: StringType
   ,selling_square_footage INT -- Scala Type: IntegerType
   ,total_square_footage INT -- Scala Type: IntegerType
   ,first_open_date DATE -- Scala Type: DateType
   ,last_remodel_date DATE -- Scala Type: DateType
   ,number_of_employees INT -- Scala Type: IntegerType
   ,annual_shrinkage INT -- Scala Type: IntegerType
   ,foot_traffic INT -- Scala Type: IntegerType
   ,monthly_rent_cost INT -- Scala Type: IntegerType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.store_dimension');
SELECT ANALYZE_STATISTICS('parquet.store_dimension');

SELECT count(*) FROM parquet.store_dimension;
SELECT * FROM parquet.store_dimension LIMIT 2;
