-- Custom data quality test for clean_aqi_data
-- This test fails if any bad data is found

with base as (
    select *
    from {{ ref('clean_aqi_data') }}
),

invalid_records as (
    select
        INDEX_RECORD_TS,
        CITY,
        STATION,
        POLLUTANT_ID,
        POLLUTANT_AVG
    from base
    where
        INDEX_RECORD_TS is null
        or CITY is null
        or STATION is null
        or POLLUTANT_ID is null
        or try_to_double(POLLUTANT_AVG) is null
        or POLLUTANT_AVG < 0
        or POLLUTANT_AVG > 1000
)

select *
from invalid_records
