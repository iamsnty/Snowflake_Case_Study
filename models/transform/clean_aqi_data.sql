{{ config(
    materialized='incremental',
    target_lag='downstream',
    warehouse='transform_wh'
) }}

with air_quality_with_rank as (
    select 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md_5,
        _copy_data_ts,
        row_number() over (
            partition by index_record_ts 
            order by _stg_file_load_ts desc
        ) as latest_file_rank
    from dev_db.stage_api_final.air_quality
    where index_record_ts is not null
),

unique_air_quality_data as (
    select * 
    from air_quality_with_rank 
    where latest_file_rank = 1
)

select 
    index_record_ts,
    hourly_rec.value:country::text as country,
    hourly_rec.value:state::text as state,
    hourly_rec.value:city::text as city,
    hourly_rec.value:station::text as station,
    hourly_rec.value:latitude::number(12,7) as latitude,
    hourly_rec.value:longitude::number(12,7) as longitude,
    hourly_rec.value:pollutant_id::text as pollutant_id,
    hourly_rec.value:pollutant_max::text as pollutant_max,
    hourly_rec.value:pollutant_min::text as pollutant_min,
    hourly_rec.value:pollutant_avg::text as pollutant_avg,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md_5,
    _copy_data_ts
from unique_air_quality_data,
     lateral flatten (input => json_data:records) hourly_rec
