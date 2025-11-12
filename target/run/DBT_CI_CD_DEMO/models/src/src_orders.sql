
  create or replace   view DBT_CI_DEMO.SRC.src_orders
  
    
    
(
  
    "O_CUSTKEY" COMMENT $$$$, 
  
    "O_ORDERKEY" COMMENT $$$$, 
  
    "O_ORDERDATE" COMMENT $$$$, 
  
    "O_TOTALPRICE" COMMENT $$$$, 
  
    "O_ORDERSTATUS" COMMENT $$$$
  
)

   as (
    WITH orderdata AS (
    SELECT * FROM snowflake_sample_data.tpch_sf1.orders
)

SELECT
    o_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    o_orderstatus
FROM
orderdata
  );

