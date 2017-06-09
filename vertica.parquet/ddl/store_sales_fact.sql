CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.store_sales_fact;
\i set_paths.sql

\set parquet_file :parquet_path /store_sales_fact.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.store_sales_fact (
    date_key INT -- Scala Type: IntegerType
   ,product_key INT -- Scala Type: IntegerType
   ,product_version INT -- Scala Type: IntegerType
   ,store_key INT -- Scala Type: IntegerType
   ,promotion_key INT -- Scala Type: IntegerType
   ,customer_key INT -- Scala Type: IntegerType
   ,employee_key INT -- Scala Type: IntegerType
   ,pos_transaction_number INT -- Scala Type: IntegerType
   ,sales_quantity INT -- Scala Type: IntegerType
   ,sales_dollar_amount INT -- Scala Type: IntegerType
   ,cost_dollar_amount INT -- Scala Type: IntegerType
   ,gross_profit_dollar_amount INT -- Scala Type: IntegerType
   ,transaction_type VARCHAR(16) -- Scala Type: StringType
   ,transaction_time VARCHAR -- Scala Type: StringType
   ,tender_type VARCHAR(8) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.store_sales_fact');
SELECT ANALYZE_STATISTICS('parquet.store_sales_fact');

SELECT count(*) FROM parquet.store_sales_fact;
SELECT * FROM parquet.store_sales_fact LIMIT 2;
