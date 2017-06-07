-- Equi join
-- Joins online_sales_fact table and the call_center_dimension 
-- table with the ON clause

SELECT sales_quantity, sales_dollar_amount, transaction_type, cc_name
FROM parquet.online_sales_fact
INNER JOIN parquet.call_center_dimension 
ON (parquet.online_sales_fact.call_center_key 
    = parquet.call_center_dimension.call_center_key
    AND sale_date_key = 156)
ORDER BY sales_dollar_amount DESC;
