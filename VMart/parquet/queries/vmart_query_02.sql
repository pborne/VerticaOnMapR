-- WHERE clause subquery
-- Asks for all orders placed by stores located in Massachusetts 
-- and by vendors located elsewhere before March 1, 2003:

SELECT order_number, date_ordered
FROM parquet.store_orders_fact orders
WHERE orders.store_key IN (
  SELECT store_key
  FROM parquet.store_dimension
  WHERE store_state = 'MA') 
AND orders.vendor_key NOT IN (
    SELECT vendor_key
    FROM parquet.vendor_dimension
    WHERE vendor_state = 'MA')
AND date_ordered < '2003-03-01';
