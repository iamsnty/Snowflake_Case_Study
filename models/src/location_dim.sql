
{{ config(
    materialized='incremental',
    target_lag='downstream',
    warehouse='transform_wh'
) }}



with step01_unique_data as (
select 
    LATITUDE,
    LONGITUDE,
    COUNTRY,
    STATE,
    CITY,
    STATION,
from 
    transform_sch.clean_flatten_aqi_data
    group by 1,2,3,4,5,6
)
select 
    hash(LATITUDE,LONGITUDE) as location_pk,
    *
from step01_unique_data
order by 
    country, STATE, city, station

