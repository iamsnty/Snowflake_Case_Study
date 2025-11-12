

{{ config(
    materialized='incremental',
    target_lag='downstream',
    warehouse='transform_wh'
) }}


with step01_city_level_data as (
select 
    d.measurement_time,
    l.country as country,
    l.state as state,
    l.city as city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    air_quality_fact f
    join date_dim d on f.date_fk = d.date_pk
    join location_dim l on f.location_fk = l.location_pk
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
    step01_city_level_data

