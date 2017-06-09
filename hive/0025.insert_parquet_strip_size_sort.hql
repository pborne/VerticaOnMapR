####################################################################
# Compression format    Hadoop CompressionCodec
# DEFLATE               org.apache.hadoop.io.compress.DefaultCodec
# gzip                  org.apache.hadoop.io.compress.GzipCodec
# bzip2                 org.apache.hadoop.io.compress.BZip2Codec
# LZO                   com.hadoop.compression.lzo.LzopCodec
# LZ4                   org.apache.hadoop.io.compress.Lz4Codec
# Snappy                org.apache.hadoop.io.compress.SnappyCodec
####################################################################

 DROP TABLE store_sales_fact_parquet_props_sort;
 CREATE TABLE store_sales_fact_parquet_props_sort (
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
 LOCATION '/mapr/vertica7/drill/hive_parquet/store_sales_fact_parquet_props_sort'
 TBLPROPERTIES ("parquet.stripe.size"="268435456");

# The difference between ORDER BY AND SORT BY is that ORDER BY guarantees global ordering
# on the entire table by using a single MapReduce reducer to populate the table.
# SORT BY uses multiple reducers, which can cause ORC or Parquet files to be sorted by
# the specified column(s) but not be globally sorted.
# Using the latter keyword can increase the time taken to load the file.

 INSERT OVERWRITE TABLE store_sales_fact_parquet_props_sort
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
 FROM store_sales_fact_csv
 SORT BY product_key,
         product_version,
         transaction_type,
         pos_transaction_number;


