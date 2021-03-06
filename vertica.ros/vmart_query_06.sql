-- EXISTS predicate
-- Orders placed by the vendor who got the best deal 
-- on January 4, 2004

SELECT store_key, order_number, date_ordered
FROM ros.store_orders_fact ord, ros.vendor_dimension vd
WHERE ord.vendor_key = vd.vendor_key
AND vd.deal_size IN (
  SELECT MAX(deal_size)
  FROM ros.vendor_dimension)
AND date_ordered = '2004-01-04';
