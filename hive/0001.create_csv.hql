#------------#
# Run in Hive
#------------#

CREATE DATABASE csv;

 DROP TABLE csv.call_center_dimension_csv;
 CREATE EXTERNAL TABLE csv.call_center_dimension_csv (
  call_center_key INT
 ,cc_closed_date STRING
 ,cc_open_date STRING
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/call_center_dimension';
 SELECT * FROM csv.call_center_dimension_csv LIMIT 5;

 DROP TABLE csv.online_page_dimension_csv;
 CREATE EXTERNAL TABLE csv.online_page_dimension_csv (
  online_page_key INT
 ,start_date STRING
 ,end_date STRING
 ,page_number INT
 ,page_description STRING
 ,page_type STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/online_page_dimension';
 SELECT * FROM csv.online_page_dimension_csv LIMIT 5;

 DROP TABLE csv.online_sales_fact_csv;
 CREATE EXTERNAL TABLE csv.online_sales_fact_csv (
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/online_sales_fact';
 SELECT * FROM csv.online_sales_fact_csv LIMIT 5;

 DROP TABLE csv.customer_dimension_csv;
 CREATE EXTERNAL TABLE csv.customer_dimension_csv (
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
 ,customer_since STRING
 ,deal_stage STRING
 ,deal_size INT
 ,last_deal_update STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/customer_dimension';
 SELECT * FROM csv.customer_dimension_csv LIMIT 5;

 DROP TABLE csv.date_dimension_csv;
 CREATE EXTERNAL TABLE csv.date_dimension_csv (
  date_key INT
 ,date_ STRING
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/date_dimension';
 SELECT * FROM csv.date_dimension_csv LIMIT 5;

 DROP TABLE csv.employee_dimension_csv;
 CREATE EXTERNAL TABLE csv.employee_dimension_csv (
  employee_key INT
 ,employee_gender STRING
 ,courtesy_title STRING
 ,employee_first_name STRING
 ,employee_middle_initial STRING
 ,employee_last_name STRING
 ,employee_age INT
 ,hire_date STRING
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/employee_dimension';
 SELECT * FROM csv.employee_dimension_csv LIMIT 5;

 DROP TABLE csv.inventory_fact_csv;
 CREATE EXTERNAL TABLE csv.inventory_fact_csv (
  date_key INT
 ,product_key INT
 ,product_version INT
 ,warehouse_key INT
 ,qty_in_stock INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/inventory_fact';
 SELECT * FROM csv.inventory_fact_csv LIMIT 5;

 DROP TABLE csv.product_dimension_csv;
 CREATE EXTERNAL TABLE csv.product_dimension_csv (
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/product_dimension';
 SELECT * FROM csv.product_dimension_csv LIMIT 5;

 DROP TABLE csv.promotion_dimension_csv;
 CREATE EXTERNAL TABLE csv.promotion_dimension_csv (
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
 ,promotion_begin_date STRING
 ,promotion_end_date STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/promotion_dimension';
 SELECT * FROM csv.promotion_dimension_csv LIMIT 5;

 DROP TABLE csv.shipping_dimension_csv;
 CREATE EXTERNAL TABLE csv.shipping_dimension_csv (
  shipping_key INT
 ,ship_type STRING
 ,ship_mode STRING
 ,ship_carrier STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/shipping_dimension';
 SELECT * FROM csv.shipping_dimension_csv LIMIT 5;

 DROP TABLE csv.vendor_dimension_csv;
 CREATE EXTERNAL TABLE csv.vendor_dimension_csv (
  vendor_key INT
 ,vendor_name STRING
 ,vendor_address STRING
 ,vendor_city STRING
 ,vendor_state STRING
 ,vendor_region STRING
 ,deal_size INT
 ,last_deal_update STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/vendor_dimension';
 SELECT * FROM csv.vendor_dimension_csv LIMIT 5;

 DROP TABLE csv.warehouse_dimension_csv;
 CREATE EXTERNAL TABLE csv.warehouse_dimension_csv (
  warehouse_key INT
 ,warehouse_name STRING
 ,warehouse_address STRING
 ,warehouse_city STRING
 ,warehouse_state STRING
 ,warehouse_region STRING
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/warehouse_dimension';
 SELECT * FROM csv.warehouse_dimension_csv LIMIT 5;

 DROP TABLE csv.store_dimension_csv;
 CREATE EXTERNAL TABLE csv.store_dimension_csv (
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
 ,first_open_date STRING
 ,last_remodel_date STRING
 ,number_of_employees INT
 ,annual_shrinkage INT
 ,foot_traffic INT
 ,monthly_rent_cost INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/store_dimension';
 SELECT * FROM csv.store_dimension_csv LIMIT 5;

 DROP TABLE csv.store_orders_fact_csv;
 CREATE EXTERNAL TABLE csv.store_orders_fact_csv (
  product_key INT
 ,product_version INT
 ,store_key INT
 ,vendor_key INT
 ,employee_key INT
 ,order_number INT
 ,date_ordered STRING
 ,date_shipped STRING
 ,expected_delivery_date STRING
 ,date_delivered STRING
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/store_orders_fact';
 SELECT * FROM csv.store_orders_fact_csv LIMIT 5;

 DROP TABLE csv.store_sales_fact_csv;
 CREATE EXTERNAL TABLE csv.store_sales_fact_csv (
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
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/mapr/vertica/vmart_csv/store_sales_fact';
 SELECT * FROM csv.store_sales_fact_csv LIMIT 5;
 

