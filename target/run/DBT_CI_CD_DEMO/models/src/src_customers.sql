
  create or replace   view DBT_CI_DEMO.SRC.src_customers
  
    
    
(
  
    "C_CUSTKEY" COMMENT $$This is a unique identifier for a given customer$$, 
  
    "C_MKTSEGMENT" COMMENT $$This is market segment of a given customer$$
  
)

   as (
    WITH custdata AS (
    SELECT *
    FROM snowflake_sample_data.tpch_sf1.customer
)

SELECT
    c_custkey,
    c_mktsegment
FROM custdata
  );

