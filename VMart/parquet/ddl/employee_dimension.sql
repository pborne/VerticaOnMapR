CREATE SCHEMA IF NOT EXISTS parquet;

DROP TABLE IF EXISTS parquet.employee_dimension;
\i set_paths.sql

\set parquet_file :parquet_path /employee_dimension.parquet/*.parquet'

CREATE EXTERNAL TABLE parquet.employee_dimension (
    employee_key INT -- Scala Type: IntegerType
   ,employee_gender VARCHAR(8) -- Scala Type: StringType
   ,courtesy_title VARCHAR(8) -- Scala Type: StringType
   ,employee_first_name VARCHAR(64) -- Scala Type: StringType
   ,employee_middle_initial VARCHAR(8) -- Scala Type: StringType
   ,employee_last_name VARCHAR(64) -- Scala Type: StringType
   ,employee_age INT -- Scala Type: IntegerType
   ,hire_date DATE -- Scala Type: DateType
   ,employee_street_address VARCHAR(256) -- Scala Type: StringType
   ,employee_city VARCHAR(64) -- Scala Type: StringType
   ,employee_state VARCHAR(2) -- Scala Type: StringType
   ,employee_region VARCHAR(32) -- Scala Type: StringType
   ,job_title VARCHAR(64) -- Scala Type: StringType
   ,reports_to INT -- Scala Type: IntegerType
   ,salaried_flag INT -- Scala Type: IntegerType
   ,annual_salary INT -- Scala Type: IntegerType
   ,hourly_rate FLOAT -- Scala Type: FloatType
   ,vacation_days INT -- Scala Type: IntegerType
)
AS COPY FROM :parquet_file
ON ANY NODE PARQUET;

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.employee_dimension');
SELECT ANALYZE_STATISTICS('parquet.employee_dimension');

SELECT count(*) FROM parquet.employee_dimension;
SELECT * FROM parquet.employee_dimension LIMIT 2;
