
 INSERT OVERWRITE TABLE call_center_dimension_parquet
 SELECT call_center_key
 ,cast(substring(from_unixtime(unix_timestamp(cc_closed_date,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(cc_open_date,'MM/dd/yyyy')),1,10) AS date)
 ,cc_name
 ,cc_class
 ,cc_employees
 ,cc_hours
 ,cc_manager
 ,cc_address
 ,cc_city
 ,cc_state
 ,cc_region
 FROM csv.call_center_dimension_csv;

 INSERT OVERWRITE TABLE online_page_dimension_parquet
 SELECT online_page_key
 ,cast(substring(from_unixtime(unix_timestamp(start_date,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(end_date,'MM/dd/yyyy')),1,10) AS date)
 ,page_number
 ,page_description
 ,page_type
 FROM csv.online_page_dimension_csv;

 INSERT OVERWRITE TABLE online_sales_fact_parquet
 SELECT sale_date_key
 ,ship_date_key
 ,product_key
 ,product_version
 ,customer_key
 ,call_center_key
 ,online_page_key
 ,shipping_key
 ,warehouse_key
 ,promotion_key
 ,pos_transaction_number
 ,sales_quantity
 ,sales_dollar_amount
 ,ship_dollar_amount
 ,net_dollar_amount
 ,cost_dollar_amount
 ,gross_profit_dollar_amount
 ,transaction_type
 FROM csv.online_sales_fact_csv;

 INSERT OVERWRITE TABLE customer_dimension_parquet
 SELECT customer_key
 ,customer_type
 ,customer_name
 ,customer_gender
 ,title
 ,household_id
 ,customer_address
 ,customer_city
 ,customer_state
 ,customer_region
 ,marital_status
 ,customer_age
 ,number_of_children
 ,annual_income
 ,occupation
 ,largest_bill_amount
 ,store_membership_card
 ,cast(substring(from_unixtime(unix_timestamp(customer_since,'MM/dd/yyyy')),1,10) AS date)
 ,deal_stage
 ,deal_size
 ,cast(substring(from_unixtime(unix_timestamp(last_deal_update,'MM/dd/yyyy')),1,10) AS date)
 FROM csv.customer_dimension_csv;

 INSERT OVERWRITE TABLE date_dimension_parquet
 SELECT date_key
 ,cast(substring(from_unixtime(unix_timestamp(date_,'MM/dd/yyyy')),1,10) AS date)
 ,full_date_description
 ,day_of_week
 ,day_number_in_calendar_month
 ,day_number_in_calendar_year
 ,day_number_in_fiscal_month
 ,day_number_in_fiscal_year
 ,last_day_in_week_indicator
 ,last_day_in_month_indicator
 ,calendar_week_number_in_year
 ,calendar_month_name
 ,calendar_month_number_in_year
 ,calendar_year_month
 ,calendar_quarter
 ,calendar_year_quarter
 ,calendar_half_year
 ,calendar_year
 ,holiday_indicator
 ,weekday_indicator
 ,selling_season
 FROM csv.date_dimension_csv;

 INSERT OVERWRITE TABLE employee_dimension_parquet
 SELECT employee_key
 ,employee_gender
 ,courtesy_title
 ,employee_first_name
 ,employee_middle_initial
 ,employee_last_name
 ,employee_age
 ,cast(substring(from_unixtime(unix_timestamp(hire_date,'MM/dd/yyyy')),1,10) AS date)
 ,employee_street_address
 ,employee_city
 ,employee_state
 ,employee_region
 ,job_title
 ,reports_to
 ,salaried_flag
 ,annual_salary
 ,hourly_rate
 ,vacation_days
 FROM csv.employee_dimension_csv;

 INSERT OVERWRITE TABLE inventory_fact_parquet
 SELECT date_key
 ,product_key
 ,product_version
 ,warehouse_key
 ,qty_in_stock
 FROM csv.inventory_fact_csv;

 INSERT OVERWRITE TABLE product_dimension_parquet
 SELECT product_key
 ,product_version
 ,product_description
 ,sku_number
 ,category_description
 ,department_description
 ,package_type_description
 ,package_size
 ,fat_content
 ,diet_type
 ,weight
 ,weight_units_of_measure
 ,shelf_width
 ,shelf_height
 ,shelf_depth
 ,product_price
 ,product_cost
 ,lowest_competitor_price
 ,highest_competitor_price
 ,average_competitor_price
 ,discontinued_flag
 FROM csv.product_dimension_csv;

 INSERT OVERWRITE TABLE promotion_dimension_parquet
 SELECT promotion_key
 ,promotion_name
 ,price_reduction_type
 ,promotion_media_type
 ,ad_type
 ,display_type
 ,coupon_type
 ,ad_media_name
 ,display_provider
 ,promotion_cost
 ,cast(substring(from_unixtime(unix_timestamp(promotion_begin_date,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(promotion_end_date,'MM/dd/yyyy')),1,10) AS date)
 FROM csv.promotion_dimension_csv;

 INSERT OVERWRITE TABLE shipping_dimension_parquet
 SELECT shipping_key
 ,ship_type
 ,ship_mode
 ,ship_carrier
 FROM csv.shipping_dimension_csv;

 INSERT OVERWRITE TABLE vendor_dimension_parquet
 SELECT vendor_key
 ,vendor_name
 ,vendor_address
 ,vendor_city
 ,vendor_state
 ,vendor_region
 ,deal_size
 ,cast(substring(from_unixtime(unix_timestamp(last_deal_update,'MM/dd/yyyy')),1,10) AS date)
 FROM csv.vendor_dimension_csv;

 INSERT OVERWRITE TABLE warehouse_dimension_parquet
 SELECT warehouse_key
 ,warehouse_name
 ,warehouse_address
 ,warehouse_city
 ,warehouse_state
 ,warehouse_region
 FROM csv.warehouse_dimension_csv;

 INSERT OVERWRITE TABLE store_dimension_parquet
 SELECT store_key
 ,store_name
 ,store_number
 ,store_address
 ,store_city
 ,store_state
 ,store_region
 ,floor_plan_type
 ,photo_processing_type
 ,financial_service_type
 ,selling_square_footage
 ,total_square_footage
 ,cast(substring(from_unixtime(unix_timestamp(first_open_date,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(last_remodel_date,'MM/dd/yyyy')),1,10) AS date)
 ,number_of_employees
 ,annual_shrinkage
 ,foot_traffic
 ,monthly_rent_cost
 FROM csv.store_dimension_csv;

 INSERT OVERWRITE TABLE store_orders_fact_parquet
 SELECT product_key
 ,product_version
 ,store_key
 ,vendor_key
 ,employee_key
 ,order_number
 ,cast(substring(from_unixtime(unix_timestamp(date_ordered,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(date_shipped,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(expected_delivery_date,'MM/dd/yyyy')),1,10) AS date)
 ,cast(substring(from_unixtime(unix_timestamp(date_delivered,'MM/dd/yyyy')),1,10) AS date)
 ,quantity_ordered
 ,quantity_delivered
 ,shipper_name
 ,unit_price
 ,shipping_cost
 ,total_order_cost
 ,quantity_in_stock
 ,reorder_level
 ,overstock_ceiling
 FROM csv.store_orders_fact_csv;

 INSERT OVERWRITE TABLE store_sales_fact_parquet
 SELECT date_key
 ,product_key
 ,product_version
 ,store_key
 ,promotion_key
 ,customer_key
 ,employee_key
 ,pos_transaction_number
 ,sales_quantity
 ,sales_dollar_amount
 ,cost_dollar_amount
 ,gross_profit_dollar_amount
 ,transaction_type
 ,transaction_time
 ,tender_type
 FROM csv.store_sales_fact_csv;

