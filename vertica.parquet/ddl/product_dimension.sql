CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.product_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /product_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.product_dimension (
    product_key INT -- Scala Type: IntegerType
   ,product_version INT -- Scala Type: IntegerType
   ,product_description VARCHAR(128) -- Scala Type: StringType
   ,sku_number VARCHAR(32) -- Scala Type: StringType
   ,category_description VARCHAR(32) -- Scala Type: StringType
   ,department_description VARCHAR(32) -- Scala Type: StringType
   ,package_type_description VARCHAR(32) -- Scala Type: StringType
   ,package_size VARCHAR(32) -- Scala Type: StringType
   ,fat_content INT -- Scala Type: IntegerType
   ,diet_type VARCHAR(32) -- Scala Type: StringType
   ,weight INT -- Scala Type: IntegerType
   ,weight_units_of_measure VARCHAR(32) -- Scala Type: StringType
   ,shelf_width INT -- Scala Type: IntegerType
   ,shelf_height INT -- Scala Type: IntegerType
   ,shelf_depth INT -- Scala Type: IntegerType
   ,product_price INT -- Scala Type: IntegerType
   ,product_cost INT -- Scala Type: IntegerType
   ,lowest_competitor_price INT -- Scala Type: IntegerType
   ,highest_competitor_price INT -- Scala Type: IntegerType
   ,average_competitor_price INT -- Scala Type: IntegerType
   ,discontinued_flag INT -- Scala Type: IntegerType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.product_dimension');
SELECT ANALYZE_STATISTICS('parquet.product_dimension');

SELECT count(*) FROM parquet.product_dimension;
SELECT * FROM parquet.product_dimension LIMIT 2;
