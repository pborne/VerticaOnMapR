-- EXISTS predicate
-- Get a list of all the orders placed by all stores on 
-- January 2, 2003 for the vendors with records in the 
-- vendor_dimension table 

SELECT store_key, order_number, date_ordered
FROM ros.store_orders_fact
WHERE EXISTS (
  SELECT 1
  FROM ros.vendor_dimension
  WHERE ros.vendor_dimension.vendor_key = ros.store_orders_fact.vendor_key)
  AND date_ordered = '2003-01-02';
