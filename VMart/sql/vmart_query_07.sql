-- Multicolumn subquery
-- Which products have the highest cost, 
-- grouped by category and department 

SELECT product_description, sku_number, department_description
FROM ros.product_dimension
WHERE (category_description, department_description, product_cost) IN (
  SELECT category_description, department_description, 
  MAX(product_cost) FROM ros.product_dimension
  GROUP BY category_description, department_description);
