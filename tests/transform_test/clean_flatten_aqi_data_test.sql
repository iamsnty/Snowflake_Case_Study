-- Test: Data quality validation for clean_aqi_data

with validation as (
    select *
    from {{ ref('clean_flatten_aqi_data') }}
    where
        -- Critical fields should not be null
        city is null
        or state is null
        or station is null
        or latitude is null
        or longitude is null

        -- Invalid coordinate ranges
        or latitude not between -90 and 90
        or longitude not between -180 and 180

        -- Pollutant averages should not be negative
        or pm25_avg < 0
        or pm10_avg < 0
        or so2_avg < 0
        or no2_avg < 0
        or nh3_avg < 0
        or co_avg < 0
        or o3_avg < 0
)

select *
from validation
