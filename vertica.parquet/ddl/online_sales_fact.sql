CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.online_sales_fact;
\i set_paths.sql

\set parquet_file :parquet_path /online_sales_fact.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.online_sales_fact (
    sale_date_key INT -- Scala Type: IntegerType
   ,ship_date_key INT -- Scala Type: IntegerType
   ,product_key INT -- Scala Type: IntegerType
   ,product_version INT -- Scala Type: IntegerType
   ,customer_key INT -- Scala Type: IntegerType
   ,call_center_key INT -- Scala Type: IntegerType
   ,online_page_key INT -- Scala Type: IntegerType
   ,shipping_key INT -- Scala Type: IntegerType
   ,warehouse_key INT -- Scala Type: IntegerType
   ,promotion_key INT -- Scala Type: IntegerType
   ,pos_transaction_number INT -- Scala Type: IntegerType
   ,sales_quantity INT -- Scala Type: IntegerType
   ,sales_dollar_amount FLOAT -- Scala Type: FloatType
   ,ship_dollar_amount FLOAT -- Scala Type: FloatType
   ,net_dollar_amount FLOAT -- Scala Type: FloatType
   ,cost_dollar_amount FLOAT -- Scala Type: FloatType
   ,gross_profit_dollar_amount FLOAT -- Scala Type: FloatType
   ,transaction_type VARCHAR(16) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.online_sales_fact');
SELECT ANALYZE_STATISTICS('parquet.online_sales_fact');

SELECT count(*) FROM parquet.online_sales_fact;
SELECT * FROM parquet.online_sales_fact LIMIT 2;
