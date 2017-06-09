 CREATE DATABASE parquet;

 DROP TABLE parquet.call_center_dimension_parquet;
 CREATE TABLE parquet.call_center_dimension_parquet (
  call_center_key INT
 ,cc_closed_date DATE
 ,cc_open_date DATE
 ,cc_name STRING
 ,cc_class STRING
 ,cc_employees INT
 ,cc_hours STRING
 ,cc_manager STRING
 ,cc_address STRING
 ,cc_city STRING
 ,cc_state STRING
 ,cc_region STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/call_center_dimension';

 DROP TABLE parquet.online_page_dimension_parquet;
 CREATE TABLE parquet.online_page_dimension_parquet (
  online_page_key INT
 ,start_date DATE
 ,end_date DATE
 ,page_number INT
 ,page_description STRING
 ,page_type STRING
 )
 STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/online_page_dimension';

 DROP TABLE parquet.online_sales_fact_parquet;
 CREATE TABLE parquet.online_sales_fact_parquet (
  sale_date_key INT
 ,ship_date_key INT
 ,product_key INT
 ,product_version INT
 ,customer_key INT
 ,call_center_key INT
 ,online_page_key INT
 ,shipping_key INT
 ,warehouse_key INT
 ,promotion_key INT
 ,pos_transaction_number INT
 ,sales_quantity INT
 ,sales_dollar_amount DOUBLE
 ,ship_dollar_amount DOUBLE
 ,net_dollar_amount DOUBLE
 ,cost_dollar_amount DOUBLE
 ,gross_profit_dollar_amount DOUBLE
 ,transaction_type STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/online_sales_fact';

 DROP TABLE parquet.customer_dimension_parquet;
 CREATE TABLE parquet.customer_dimension_parquet (
  customer_key INT
 ,customer_type STRING
 ,customer_name STRING
 ,customer_gender STRING
 ,title STRING
 ,household_id INT
 ,customer_address STRING
 ,customer_city STRING
 ,customer_state STRING
 ,customer_region STRING
 ,marital_status STRING
 ,customer_age INT
 ,number_of_children INT
 ,annual_income INT
 ,occupation STRING
 ,largest_bill_amount INT
 ,store_membership_card INT
 ,customer_since DATE
 ,deal_stage STRING
 ,deal_size INT
 ,last_deal_update DATE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/customer_dimension';

 DROP TABLE parquet.date_dimension_parquet;
 CREATE TABLE parquet.date_dimension_parquet (
  date_key INT
 ,date_ DATE
 ,full_date_description STRING
 ,day_of_week STRING
 ,day_number_in_calendar_month INT
 ,day_number_in_calendar_year INT
 ,day_number_in_fiscal_month INT
 ,day_number_in_fiscal_year INT
 ,last_day_in_week_indicator INT
 ,last_day_in_month_indicator INT
 ,calendar_week_number_in_year INT
 ,calendar_month_name STRING
 ,calendar_month_number_in_year INT
 ,calendar_year_month STRING
 ,calendar_quarter INT
 ,calendar_year_quarter STRING
 ,calendar_half_year INT
 ,calendar_year INT
 ,holiday_indicator STRING
 ,weekday_indicator STRING
 ,selling_season STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/date_dimension';

 DROP TABLE parquet.employee_dimension_parquet;
 CREATE TABLE parquet.employee_dimension_parquet (
  employee_key INT
 ,employee_gender STRING
 ,courtesy_title STRING
 ,employee_first_name STRING
 ,employee_middle_initial STRING
 ,employee_last_name STRING
 ,employee_age INT
 ,hire_date DATE
 ,employee_street_address STRING
 ,employee_city STRING
 ,employee_state STRING
 ,employee_region STRING
 ,job_title STRING
 ,reports_to INT
 ,salaried_flag INT
 ,annual_salary INT
 ,hourly_rate DOUBLE
 ,vacation_days INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/employee_dimension';

 DROP TABLE parquet.inventory_fact_parquet;
 CREATE TABLE parquet.inventory_fact_parquet (
  date_key INT
 ,product_key INT
 ,product_version INT
 ,warehouse_key INT
 ,qty_in_stock INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/inventory_fact';

 DROP TABLE parquet.product_dimension_parquet;
 CREATE TABLE parquet.product_dimension_parquet (
  product_key INT
 ,product_version INT
 ,product_description STRING
 ,sku_number STRING
 ,category_description STRING
 ,department_description STRING
 ,package_type_description STRING
 ,package_size STRING
 ,fat_content INT
 ,diet_type STRING
 ,weight INT
 ,weight_units_of_measure STRING
 ,shelf_width INT
 ,shelf_height INT
 ,shelf_depth INT
 ,product_price INT
 ,product_cost INT
 ,lowest_competitor_price INT
 ,highest_competitor_price INT
 ,average_competitor_price INT
 ,discontinued_flag INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/product_dimension';

 DROP TABLE parquet.promotion_dimension_parquet;
 CREATE TABLE parquet.promotion_dimension_parquet (
  promotion_key INT
 ,promotion_name STRING
 ,price_reduction_type STRING
 ,promotion_media_type STRING
 ,ad_type STRING
 ,display_type STRING
 ,coupon_type STRING
 ,ad_media_name STRING
 ,display_provider STRING
 ,promotion_cost INT
 ,promotion_begin_date DATE
 ,promotion_end_date DATE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/promotion_dimension';

 DROP TABLE parquet.shipping_dimension_parquet;
 CREATE TABLE parquet.shipping_dimension_parquet (
  shipping_key INT
 ,ship_type STRING
 ,ship_mode STRING
 ,ship_carrier STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/shipping_dimension';

 DROP TABLE parquet.test_dates_parquet;
 CREATE TABLE parquet.test_dates_parquet (
  start_date DATE
 ,end_date DATE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/test_dates';

 DROP TABLE parquet.vendor_dimension_parquet;
 CREATE TABLE parquet.vendor_dimension_parquet (
  vendor_key INT
 ,vendor_name STRING
 ,vendor_address STRING
 ,vendor_city STRING
 ,vendor_state STRING
 ,vendor_region STRING
 ,deal_size INT
 ,last_deal_update DATE
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/vendor_dimension';

 DROP TABLE parquet.warehouse_dimension_parquet;
 CREATE TABLE parquet.warehouse_dimension_parquet (
  warehouse_key INT
 ,warehouse_name STRING
 ,warehouse_address STRING
 ,warehouse_city STRING
 ,warehouse_state STRING
 ,warehouse_region STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/warehouse_dimension';

 DROP TABLE parquet.store_dimension_parquet;
 CREATE TABLE parquet.store_dimension_parquet (
  store_key INT
 ,store_name STRING
 ,store_number INT
 ,store_address STRING
 ,store_city STRING
 ,store_state STRING
 ,store_region STRING
 ,floor_plan_type STRING
 ,photo_processing_type STRING
 ,financial_service_type STRING
 ,selling_square_footage INT
 ,total_square_footage INT
 ,first_open_date DATE
 ,last_remodel_date DATE
 ,number_of_employees INT
 ,annual_shrinkage INT
 ,foot_traffic INT
 ,monthly_rent_cost INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/store_dimension';

 DROP TABLE parquet.store_orders_fact_parquet;
 CREATE TABLE parquet.store_orders_fact_parquet (
  product_key INT
 ,product_version INT
 ,store_key INT
 ,vendor_key INT
 ,employee_key INT
 ,order_number INT
 ,date_ordered DATE
 ,date_shipped DATE
 ,expected_delivery_date DATE
 ,date_delivered DATE
 ,quantity_ordered INT
 ,quantity_delivered INT
 ,shipper_name STRING
 ,unit_price INT
 ,shipping_cost INT
 ,total_order_cost INT
 ,quantity_in_stock INT
 ,reorder_level INT
 ,overstock_ceiling INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET
 LOCATION '/mapr/vertica/parquet/store_orders_fact';

 DROP TABLE parquet.store_sales_fact_parquet;
 CREATE TABLE parquet.store_sales_fact_parquet (
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
 LOCATION '/mapr/vertica/parquet/store_sales_fact';

