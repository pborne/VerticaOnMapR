\set t_pwd `pwd`

\set input_file '''':t_pwd'/Date_Dimension.tbl'''
COPY ros.Date_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Product_Dimension.tbl'''
COPY ros.Product_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Store_Dimension.tbl'''
COPY ros.Store_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Promotion_Dimension.tbl'''
COPY ros.Promotion_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Vendor_Dimension.tbl'''
COPY ros.Vendor_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Customer_Dimension.tbl'''
COPY ros.Customer_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Employee_Dimension.tbl'''
COPY ros.Employee_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Warehouse_Dimension.tbl'''
COPY ros.Warehouse_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Shipping_Dimension.tbl'''
COPY ros.Shipping_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Online_Page_Dimension.tbl'''
COPY ros.Online_Page_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Call_Center_Dimension.tbl'''
COPY ros.Call_Center_Dimension FROM :input_file DELIMITER '|' NULL '' DIRECT;

\set input_file '''':t_pwd'/Store_Sales_Fact_*.tbl'''
COPY ros.Store_Sales_Fact FROM :input_file DELIMITER '|' NULL '' ON ANY NODE DIRECT;

\set input_file '''':t_pwd'/Store_Orders_Fact_*.tbl'''
COPY ros.Store_Orders_Fact FROM :input_file DELIMITER '|' NULL '' ON ANY NODE  DIRECT;

\set input_file '''':t_pwd'/Online_Sales_Fact_*.tbl'''
COPY ros.Online_Sales_Fact FROM :input_file DELIMITER '|' NULL '' ON ANY NODE  DIRECT;

\set input_file '''':t_pwd'/Inventory_Fact_*.tbl'''
COPY ros.Inventory_Fact FROM :input_file DELIMITER '|' NULL '' ON ANY NODE  DIRECT;

