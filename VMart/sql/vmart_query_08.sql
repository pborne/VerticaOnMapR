-- Using pre-join projections to answer subqueries
-- between online_sales_fact and online_page_dimension

SELECT page_description, page_type, start_date, end_date
FROM ros.online_sales_fact f, ros.online_page_dimension d
WHERE f.online_page_key = d.online_page_key 
AND page_number IN
  (SELECT MAX(page_number)
    FROM ros.online_page_dimension)
AND page_type = 'monthly'
AND start_date = '2003-06-02';
