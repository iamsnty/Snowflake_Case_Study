WITH custdata AS (
    SELECT *
    FROM snowflake_sample_data.tpch_sf1.customer
)

SELECT
    c_custkey,
    c_mktsegment
FROM custdata