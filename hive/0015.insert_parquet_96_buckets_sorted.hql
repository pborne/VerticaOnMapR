#################################################################
# Run in Hive: Bucket the table into 96 buckets + sort the data #
#################################################################

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

 DROP TABLE store_sales_fact_parquet_bucket;
 CREATE TABLE store_sales_fact_parquet_bucket (
  date_key INT
 ,product_key INT
 ,product_version INT
 ,store_key INT
 ,promotion_key INT
 ,customer_key INT
 ,employee_key INT
 ,pos_transaction_number INT
 ,sales_quantity INT
 ,sales_dollar_amount INT
 ,cost_dollar_amount INT
 ,gross_profit_dollar_amount INT
 ,transaction_type STRING
 ,transaction_time STRING
 ,tender_type STRING
 )
 CLUSTERED BY (product_key, product_version)
 SORTED BY (product_key,
          product_version,
          transaction_type,
          pos_transaction_number) INTO 96 BUCKETS
 STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/store_sales_fact_bucket';
       
set hive.enforce.bucketing = true;

 INSERT OVERWRITE TABLE store_sales_fact_parquet_bucket
 SELECT date_key
 ,product_key
 ,product_version
 ,store_key
 ,promotion_key
 ,customer_key
 ,employee_key
 ,pos_transaction_number
 ,sales_quantity
 ,sales_dollar_amount
 ,cost_dollar_amount
 ,gross_profit_dollar_amount
 ,transaction_type
 ,transaction_time
 ,tender_type
 FROM store_sales_fact_csv;

