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
