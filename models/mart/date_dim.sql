
{{ config(
    materialized='incremental',
    unique_key='date_pk',
    target_lag='downstream',
    warehouse='transform_wh'
) }}


with step01_hr_data as (
    select distinct
        index_record_ts as measurement_time,
        year(index_record_ts) as aqi_year,
        month(index_record_ts) as aqi_month,
        quarter(index_record_ts) as aqi_quarter,
        day(index_record_ts) as aqi_day,
        hour(index_record_ts) as aqi_hour
    from transform_sch.clean_flatten_aqi_data
)

select 
    hash(measurement_time) as date_pk,
    *
from step01_hr_data
order by aqi_year, aqi_month, aqi_day, aqi_hour
