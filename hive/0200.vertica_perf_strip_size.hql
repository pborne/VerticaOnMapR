#######################################
# Create an external table in Vertica #
#######################################

 DROP TABLE parquet.store_sales_fact_parquet_props;
 CREATE EXTERNAL TABLE parquet.store_sales_fact_parquet_props (
        date_key int
       ,product_key int
       ,product_version int
       ,store_key int
       ,promotion_key int
       ,customer_key int
       ,employee_key int
       ,pos_transaction_number int
       ,sales_quantity int
       ,sales_dollar_amount int
       ,cost_dollar_amount int
       ,gross_profit_dollar_amount int
       ,transaction_type varchar(16)
       ,transaction_time varchar(16) -- Modified to accomodate Hive. Should be time
       ,tender_type varchar(8)
 ) AS COPY FROM '/mapr/vertica/hive_parquet/store_sales_fact_parquet_props/*'
 ON ANY NODE PARQUET;

 SELECT * FROM parquet.store_sales_fact_parquet_props limit 5;

######################
# Compute Statistics #
######################

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.store_sales_fact_parquet_props');
SELECT ANALYZE_STATISTICS('parquet.store_sales_fact_parquet_props');

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.product_dimension');
SELECT ANALYZE_STATISTICS('parquet.product_dimension');

SELECT ANALYZE_EXTERNAL_ROW_COUNT('parquet.store_dimension');
SELECT ANALYZE_STATISTICS('parquet.store_dimension');

#########################
# Run the Vertica query #
#########################

\timing
\o /dev/null

SELECT DISTINCT s.product_key, p.product_description
FROM parquet.store_sales_fact_parquet_props s,
     parquet.product_dimension p
WHERE s.product_key = p.product_key
AND   s.product_version = p.product_version
AND   s.store_key IN (
  SELECT store_key
  FROM parquet.store_dimension
  WHERE store_state = 'MA')
ORDER BY s.product_key;

V7        : Time: First fetch (1000 rows): 26882.553 ms. All rows formatted: 31257.147 ms
V7        : Time: First fetch (1000 rows): 27124.480 ms. All rows formatted: 31461.221 ms
V8 + Stats: Time: First fetch (1000 rows): 26605.988 ms. All rows formatted: 30913.448 ms
V8 + Stats: Time: First fetch (1000 rows): 27457.991 ms. All rows formatted: 31786.819 ms

##################
# Execution Plan #
##################

Version 7:
---------

explain
 SELECT DISTINCT s.product_key, p.product_description
 FROM parquet.store_sales_fact_parquet_props s,
      parquet.product_dimension p
 WHERE s.product_key = p.product_key
 AND   s.product_version = p.product_version
 AND   s.store_key IN (
   SELECT store_key
   FROM parquet.store_dimension
   WHERE store_state = 'MA')
 ORDER BY s.product_key;

 Access Path:
 +-GROUPBY HASH (SORT OUTPUT) (GLOBAL RESEGMENT GROUPS) (LOCAL RESEGMENT GROUPS) [Cost: 16M, Rows: 100M] (PATH ID: 2)
 |  Group By: s.product_key, p.product_description
 |  Execute on: All Nodes
 | +---> JOIN HASH [Cost: 989K, Rows: 250M] (PATH ID: 3) Inner (BROADCAST)
 | |      Join Cond: (s.product_key = p.product_key) AND (s.product_version = p.product_version)
 | |      Execute on: All Nodes
 | | +-- Outer -> JOIN HASH [Semi] [Cost: 163K, Rows: 250M] (PUSHED GROUPING) (PATH ID: 4) Outer (LOCAL ROUND ROBIN) Inner (BROADCAST)
 | | |      Join Cond: (s.store_key = VAL(3))
 | | |      Execute on: All Nodes
 | | | +-- Outer -> LOAD  EXTERNAL TABLE [Cost: 0, Rows: 500M] (PUSHED GROUPING) (PATH ID: 5)
 | | | |      Table: store_sales_fact_parquet_props
 | | | |      COPY FROM '/mapr/vertica7/drill/hive_parquet/store_sales_fact_parquet_props/*' ON ANY NODE PARQUET
 | | | |      Execute on: All Nodes
 | | | |      Runtime Filters: (SIP4(HashJoin): s.store_key),
 | | | |                       (SIP1(HashJoin): s.product_key),
 | | | |                       (SIP2(HashJoin): s.product_version),
 | | | |                       (SIP3(HashJoin): s.product_key, s.product_version)
 | | | +-- Inner -> SELECT [Cost: 0, Rows: 25K] (PUSHED GROUPING) (PATH ID: 6)
 | | | |      Execute on: v_vmart_node0002
 | | | | +---> LOAD  EXTERNAL TABLE [Cost: 0, Rows: 25K] (PATH ID: 7)
 | | | | |      Table: store_dimension
 | | | | |      COPY FROM '/mapr/vertica7/drill/hive_parquet/store_dimension/*' ON ANY NODE PARQUET
 | | | | |      Filter: (store_dimension.store_state = 'MA')
 | | | | |      Execute on: v_vmart_node0002
 | | +-- Inner -> LOAD  EXTERNAL TABLE [Cost: 0, Rows: 6M] (PUSHED GROUPING) (PATH ID: 8)
 | | |      Table: product_dimension
 | | |      COPY FROM '/mapr/vertica7/drill/hive_parquet/product_dimension/*' ON ANY NODE PARQUET
 | | |      Execute on: All Nodes


Version 8 + Stats:
-----------------

 Access Path:
 +-GROUPBY HASH (SORT OUTPUT) (GLOBAL RESEGMENT GROUPS) (LOCAL RESEGMENT GROUPS) [Cost: 16M, Rows: 100M] (PATH ID: 2)
 |  Group By: s.product_key, p.product_description
 |  Execute on: All Nodes
 | +---> JOIN HASH [Cost: 989K, Rows: 250M] (PATH ID: 3) Inner (BROADCAST)
 | |      Join Cond: (s.product_key = p.product_key) AND (s.product_version = p.product_version)
 | |      Execute on: All Nodes
 | | +-- Outer -> JOIN HASH [Semi] [Cost: 163K, Rows: 250M] (PUSHED GROUPING) (PATH ID: 4) Outer (LOCAL ROUND ROBIN) Inner (BROADCAST)
 | | |      Join Cond: (s.store_key = VAL(3))
 | | |      Execute on: All Nodes
 | | | +-- Outer -> LOAD  EXTERNAL TABLE [Cost: 0, Rows: 500M] (PUSHED GROUPING) (PATH ID: 5)
 | | | |      Table: store_sales_fact_parquet_props
 | | | |      COPY FROM '/mapr/vertica7/drill/hive_parquet/store_sales_fact_parquet_props/*'  ON ANY NODE PARQUET
 | | | |      Execute on: All Nodes
 | | | |      Runtime Filters: (SIP4(HashJoin): s.store_key), (SIP1(HashJoin): s.product_key), (SIP2(HashJoin): s.product_version), (SIP3(HashJoin): s.product_key, s.product_version)
 | | | +-- Inner -> SELECT [Cost: 0, Rows: 25K] (PUSHED GROUPING) (PATH ID: 6)
 | | | |      Execute on: v_test_v8_node0001
 | | | | +---> LOAD  EXTERNAL TABLE [Cost: 0, Rows: 25K] (PATH ID: 7)
 | | | | |      Table: store_dimension
 | | | | |      COPY FROM '/mapr/vertica7/drill/hive_parquet/store_dimension/*'  ON ANY NODE PARQUET
 | | | | |      Filter: (store_dimension.store_state = 'MA')
 | | | | |      Execute on: v_test_v8_node0001
 | | +-- Inner -> LOAD  EXTERNAL TABLE [Cost: 0, Rows: 6M] (PUSHED GROUPING) (PATH ID: 8)
 | | |      Table: product_dimension
 | | |      COPY FROM '/mapr/vertica7/drill/hive_parquet/product_dimension/*'  ON ANY NODE PARQUET
 | | |      Execute on: All Nodes


