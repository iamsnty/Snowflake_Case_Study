
{{ config(
    materialized='incremental',
    target_lag='downstream',
    warehouse='transform_wh'
) }}


with step01_hr_data as (
select 
        index_record_ts as measurement_time,
        year(index_record_ts) as aqi_year,
        month(index_record_ts) as aqi_month,
        quarter(index_record_ts) as aqi_quarter,
        day(index_record_ts) aqi_day,
        hour(index_record_ts)+1 aqi_hour,
    from 
        transform_sch.clean_flatten_aqi_data
        group by 1,2,3,4,5,6
)
select 
    hash(measurement_time) as date_pk,
    *
from step01_hr_data
order by aqi_year,aqi_month,aqi_day,aqi_hour