-- FROM clause subquery
-- Return the values for five products with the 
-- lowest-fat content in the Dairy department

SELECT fat_content 
FROM (
  SELECT DISTINCT fat_content 
  FROM ros.product_dimension 
  WHERE department_description 
  IN ('Dairy') ) AS food 
  ORDER BY fat_content
  LIMIT 5;

-- vmart_query_02.sql
-- WHERE clause subquery
-- Asks for all orders placed by stores located in Massachusetts 
-- and by vendors located elsewhere before March 1, 2003:

SELECT order_number, date_ordered
FROM ros.store_orders_fact orders
WHERE orders.store_key IN (
  SELECT store_key
  FROM ros.store_dimension
  WHERE store_state = 'MA') 
AND orders.vendor_key NOT IN (
    SELECT vendor_key
    FROM ros.vendor_dimension
    WHERE vendor_state = 'MA')
AND date_ordered < '2003-03-01';

-- vmart_query_03.sql
-- Noncorrelated subquery
-- Requests female and male customers with the maximum 
-- annual income from customers

SELECT customer_name, annual_income
FROM ros.customer_dimension
WHERE (customer_gender, annual_income) IN
      (SELECT customer_gender, MAX(annual_income)
       FROM ros.customer_dimension
       GROUP BY customer_gender);

-- vmart_query_04.sql
-- IN predicate
-- Find all products supplied by stores in MA

SELECT DISTINCT s.product_key, p.product_description
FROM ros.store_sales_fact  s,
     ros.product_dimension p
WHERE s.product_key = p.product_key
  AND s.product_version = p.product_version
  AND s.store_key IN (
    SELECT store_key
    FROM ros.store_dimension
    WHERE store_state = 'MA')
ORDER BY s.product_key;

-- vmart_query_05.sql
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

-- vmart_query_06.sql
-- EXISTS predicate
-- Orders placed by the vendor who got the best deal 
-- on January 4, 2004

SELECT store_key, order_number, date_ordered
FROM ros.store_orders_fact ord,
     ros.vendor_dimension vd
WHERE ord.vendor_key = vd.vendor_key
AND vd.deal_size IN (
  SELECT MAX(deal_size)
  FROM ros.vendor_dimension)
AND date_ordered = '2004-01-04';

-- vmart_query_07.sql
-- Multicolumn subquery
-- Which products have the highest cost, 
-- grouped by category and department 

SELECT product_description, sku_number, department_description
FROM ros.product_dimension
WHERE (category_description, department_description, product_cost) IN (
  SELECT category_description, department_description, 
  MAX(product_cost)
  FROM ros.product_dimension
  GROUP BY category_description, department_description);

-- vmart_query_08.sql
-- Using pre-join projections to answer subqueries
-- between online_sales_fact and online_page_dimension

SELECT page_description, page_type, start_date, end_date
FROM ros.online_sales_fact f,
     ros.online_page_dimension d
WHERE f.online_page_key = d.online_page_key 
AND page_number IN
  (SELECT MAX(page_number)
    FROM ros.online_page_dimension)
AND page_type  = 'monthly'
AND start_date = '2003-06-02';