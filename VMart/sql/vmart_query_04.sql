-- IN predicate
-- Find all products supplied by stores in MA

SELECT DISTINCT s.product_key, p.product_description
FROM ros.store_sales_fact s, ros.product_dimension p
WHERE s.product_key = p.product_key
AND s.product_version = p.product_version
AND s.store_key IN (
  SELECT store_key
  FROM ros.store_dimension
  WHERE store_state = 'MA')
ORDER BY s.product_key;
