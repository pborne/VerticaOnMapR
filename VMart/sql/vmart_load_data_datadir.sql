\set t_pwd `pwd`

\set input_file '''':t_pwd'/Date_Dimension.csv.gz'''
COPY ros.Date_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Product_Dimension.csv.gz'''
COPY ros.Product_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Store_Dimension.csv.gz'''
COPY ros.Store_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Promotion_Dimension.csv.gz'''
COPY ros.Promotion_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Vendor_Dimension.csv.gz'''
COPY ros.Vendor_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Customer_Dimension.csv.gz'''
COPY ros.Customer_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Employee_Dimension.csv.gz'''
COPY ros.Employee_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Warehouse_Dimension.csv.gz'''
COPY ros.Warehouse_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Shipping_Dimension.csv.gz'''
COPY ros.Shipping_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Online_Page_Dimension.csv.gz'''
COPY ros.Online_Page_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Call_Center_Dimension.csv.gz'''
COPY ros.Call_Center_Dimension FROM :input_file GZIP DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Store_Sales_Fact_*.csv.gz'''
COPY ros.Store_Sales_Fact FROM :input_file GZIP DELIMITER '|' NULL '' ON ANY NODE DIRECT;

\set input_file '''':t_pwd'/Store_Orders_Fact_*.csv.gz'''
COPY ros.Store_Orders_Fact FROM :input_file GZIP DELIMITER '|' NULL '' ON ANY NODE  DIRECT;

\set input_file '''':t_pwd'/Online_Sales_Fact_*.csv.gz'''
COPY ros.Online_Sales_Fact FROM :input_file GZIP DELIMITER '|' NULL '' ON ANY NODE  DIRECT;

\set input_file '''':t_pwd'/Inventory_Fact_*.csv.gz'''
COPY ros.Inventory_Fact FROM :input_file GZIP DELIMITER '|' NULL '' ON ANY NODE  DIRECT;

