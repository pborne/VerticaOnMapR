CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.promotion_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /promotion_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.promotion_dimension (
    promotion_key INT -- Scala Type: IntegerType
   ,promotion_name VARCHAR(128) -- Scala Type: StringType
   ,price_reduction_type VARCHAR(32) -- Scala Type: StringType
   ,promotion_media_type VARCHAR(32) -- Scala Type: StringType
   ,ad_type VARCHAR(32) -- Scala Type: StringType
   ,display_type VARCHAR(32) -- Scala Type: StringType
   ,coupon_type VARCHAR(32) -- Scala Type: StringType
   ,ad_media_name VARCHAR(32) -- Scala Type: StringType
   ,display_provider VARCHAR(128) -- Scala Type: StringType
   ,promotion_cost INT -- Scala Type: IntegerType
   ,promotion_begin_date DATE -- Scala Type: DateType
   ,promotion_end_date DATE -- Scala Type: DateType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.promotion_dimension');
SELECT ANALYZE_STATISTICS('parquet.promotion_dimension');

SELECT count(*) FROM parquet.promotion_dimension;
SELECT * FROM parquet.promotion_dimension LIMIT 2;
