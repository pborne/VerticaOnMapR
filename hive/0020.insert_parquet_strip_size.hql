####################################################################
# Compression format    Hadoop CompressionCodec
# DEFLATE               org.apache.hadoop.io.compress.DefaultCodec
# gzip                  org.apache.hadoop.io.compress.GzipCodec
# bzip2                 org.apache.hadoop.io.compress.BZip2Codec
# LZO                   com.hadoop.compression.lzo.LzopCodec
# LZ4                   org.apache.hadoop.io.compress.Lz4Codec
# Snappy                org.apache.hadoop.io.compress.SnappyCodec
####################################################################

 DROP TABLE store_sales_fact_parquet_props;
 CREATE TABLE store_sales_fact_parquet_props (
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
 STORED AS PARQUET
 LOCATION '/mapr/vertica/hive_parquet/store_sales_fact_parquet_props'
 TBLPROPERTIES ("parquet.stripe.size"="268435456");

-- set hive.enforce.bucketing = true;

 INSERT OVERWRITE TABLE store_sales_fact_parquet_props
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


