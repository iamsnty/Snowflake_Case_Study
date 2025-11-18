

{{ config(
    materialized='incremental',
    target_lag='downstream',
    unique_key='aqi_pk',
    warehouse='transform_wh'
) }}

select 
        hash(index_record_ts,latitude,longitude) aqi_pk,
        hash(index_record_ts) as date_fk,
        hash(latitude,longitude) as location_fk,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_index(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when three_sub_index_criteria(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG) > 2 then greatest (PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)
        else 0
        end
    as aqi
    from transform_sch.clean_flatten_aqi_data
