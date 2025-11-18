

{{ config(
    materialized='table',
    warehouse='transform_wh'
) }}

with step01_city_day_level_data as (
select 
    date(measurement_time) as measurement_date,
    country as country,
    state as state,
    city as city,
    round(avg(pm10_avg)) as pm10_avg,
    round(avg(pm25_avg)) as pm25_avg,
    round(avg(so2_avg)) as so2_avg,
    round(avg(no2_avg)) as no2_avg,
    round(avg(nh3_avg)) as nh3_avg,
    round(avg(co_avg)) as co_avg,
    round(avg(o3_avg)) as o3_avg
from 
    agg_city_fact_hour_level
group by 
    1,2,3,4
)
select 
    *,
    prominent_index(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
from 
    step01_city_day_level_data
