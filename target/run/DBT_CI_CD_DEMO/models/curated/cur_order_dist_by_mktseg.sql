
  
    

        create or replace transient table DBT_CI_DEMO.SRC.cur_order_dist_by_mktseg
         as
        (WITH joindata AS (
    SELECT
        cust.c_custkey,
        cust.c_mktsegment,
        ordr.o_orderdate,
        ordr.o_orderkey,
        ordr.o_orderstatus,
        ordr.o_totalprice
    FROM DBT_CI_DEMO.SRC.src_customers AS cust
    INNER JOIN DBT_CI_DEMO.SRC.src_orders AS ordr
        ON cust.c_custkey = ordr.o_custkey
),

aggr AS (
    SELECT
        joindata.c_mktsegment,
        sum(joindata.o_totalprice) AS totalprice
    FROM joindata
    GROUP BY joindata.c_mktsegment
)

SELECT * FROM aggr
        );
      
  