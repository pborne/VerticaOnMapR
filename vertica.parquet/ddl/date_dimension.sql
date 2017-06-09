CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.date_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /date_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.date_dimension (
    date_key INT -- Scala Type: IntegerType
   ,date DATE -- Scala Type: DateType
   ,full_date_description VARCHAR(18) -- Scala Type: StringType
   ,day_of_week VARCHAR(9) -- Scala Type: StringType
   ,day_number_in_calendar_month INT -- Scala Type: IntegerType
   ,day_number_in_calendar_year INT -- Scala Type: IntegerType
   ,day_number_in_fiscal_month INT -- Scala Type: IntegerType
   ,day_number_in_fiscal_year INT -- Scala Type: IntegerType
   ,last_day_in_week_indicator INT -- Scala Type: IntegerType
   ,last_day_in_month_indicator INT -- Scala Type: IntegerType
   ,calendar_week_number_in_year INT -- Scala Type: IntegerType
   ,calendar_month_name VARCHAR(9) -- Scala Type: StringType
   ,calendar_month_number_in_year INT -- Scala Type: IntegerType
   ,calendar_year_month VARCHAR(7) -- Scala Type: StringType
   ,calendar_quarter INT -- Scala Type: IntegerType
   ,calendar_year_quarter VARCHAR(7) -- Scala Type: StringType
   ,calendar_half_year INT -- Scala Type: IntegerType
   ,calendar_year INT -- Scala Type: IntegerType
   ,holiday_indicator VARCHAR(10) -- Scala Type: StringType
   ,weekday_indicator VARCHAR(7) -- Scala Type: StringType
   ,selling_season VARCHAR(32) -- Scala Type: StringType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.date_dimension');
SELECT ANALYZE_STATISTICS('parquet.date_dimension');

SELECT count(*) FROM parquet.date_dimension;
SELECT * FROM parquet.date_dimension LIMIT 2;
