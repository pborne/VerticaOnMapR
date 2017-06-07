CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.customer_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /customer_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.customer_dimension (
    customer_key INT -- Scala Type: IntegerType
   ,customer_type VARCHAR(16) -- Scala Type: StringType
   ,customer_name VARCHAR(256) -- Scala Type: StringType
   ,customer_gender VARCHAR(8) -- Scala Type: StringType
   ,title VARCHAR(8) -- Scala Type: StringType
   ,household_id INT -- Scala Type: IntegerType
   ,customer_address VARCHAR(256) -- Scala Type: StringType
   ,customer_city VARCHAR(64) -- Scala Type: StringType
   ,customer_state VARCHAR(2) -- Scala Type: StringType
   ,customer_region VARCHAR(64) -- Scala Type: StringType
   ,marital_status VARCHAR(32) -- Scala Type: StringType
   ,customer_age INT -- Scala Type: IntegerType
   ,number_of_children INT -- Scala Type: IntegerType
   ,annual_income INT -- Scala Type: IntegerType
   ,occupation VARCHAR(64) -- Scala Type: StringType
   ,largest_bill_amount INT -- Scala Type: IntegerType
   ,store_membership_card INT -- Scala Type: IntegerType
   ,customer_since DATE -- Scala Type: DateType
   ,deal_stage VARCHAR(32) -- Scala Type: StringType
   ,deal_size INT -- Scala Type: IntegerType
   ,last_deal_update DATE -- Scala Type: DateType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.customer_dimension');
SELECT ANALYZE_STATISTICS('parquet.customer_dimension');

SELECT count(*) FROM parquet.customer_dimension;
SELECT * FROM parquet.customer_dimension LIMIT 2;
