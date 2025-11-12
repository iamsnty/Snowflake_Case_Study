
{{ config(
    materialized='incremental',
    target_lag='30 min',
    warehouse='transform_wh'
) }}


with step01_combine_pollutant_cte as (
    SELECT 
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN POLLUTANT_ID = 'PM10' THEN POLLUTANT_AVG END) AS PM10_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'PM2.5' THEN POLLUTANT_AVG END) AS PM25_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'SO2' THEN POLLUTANT_AVG END) AS SO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NO2' THEN POLLUTANT_AVG END) AS NO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NH3' THEN POLLUTANT_AVG END) AS NH3_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'CO' THEN POLLUTANT_AVG END) AS CO_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'OZONE' THEN POLLUTANT_AVG END) AS O3_AVG
    FROM 
        dev_db.transform_sch.clean_aqi_data
    group by 
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station
),
step02_replace_na_cte as (
    select 
        INDEX_RECORD_TS,
        COUNTRY,
        replace(STATE,'_',' ') as STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        CASE 
            WHEN PM25_AVG = 'NA' THEN 0 
            WHEN PM25_AVG is Null THEN 0 
            ELSE round(PM25_AVG)
        END as PM25_AVG,
        CASE 
            WHEN PM10_AVG = 'NA' THEN 0 
            WHEN PM10_AVG is Null THEN 0 
            ELSE round(PM10_AVG)
        END as PM10_AVG,
        CASE 
            WHEN SO2_AVG = 'NA' THEN 0 
            WHEN SO2_AVG is Null THEN 0 
            ELSE round(SO2_AVG)
        END as SO2_AVG,
        CASE 
            WHEN NO2_AVG = 'NA' THEN 0 
            WHEN NO2_AVG is Null THEN 0 
            ELSE round(NO2_AVG)
        END as NO2_AVG,
         CASE 
            WHEN NH3_AVG = 'NA' THEN 0 
            WHEN NH3_AVG is Null THEN 0 
            ELSE round(NH3_AVG)
        END as NH3_AVG,
         CASE 
            WHEN CO_AVG = 'NA' THEN 0 
            WHEN CO_AVG is Null THEN 0 
            ELSE round(CO_AVG)
        END as CO_AVG,
         CASE 
            WHEN O3_AVG = 'NA' THEN 0 
            WHEN O3_AVG is Null THEN 0 
            ELSE round(O3_AVG)
        END as O3_AVG,
    from step01_combine_pollutant_cte
)
select *,
from step02_replace_na_cte
