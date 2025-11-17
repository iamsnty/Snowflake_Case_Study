{{ config(
    materialized='incremental',
    unique_key='id',
    target_lag='downstream',
    warehouse='transform_wh'
) }}

with air_quality_with_rank as (
    select 
    id,
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md_5,
        _copy_data_ts,
        row_number() over (
            partition by id 
            order by index_record_ts desc
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
    parse_json(json_data):country::text as country,
    parse_json(json_data):state::text as state,
    parse_json(json_data):city::text as city,
    parse_json(json_data):station::text as station,
    parse_json(json_data):latitude::number(12,7) as latitude,
    parse_json(json_data):longitude::number(12,7) as longitude,
    parse_json(json_data):pollutant_id::text as pollutant_id,
    parse_json(json_data):min_value::text as pollutant_min,
    parse_json(json_data):max_value::text as pollutant_max,
    parse_json(json_data):avg_value::text as pollutant_avg,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md_5,
    _copy_data_ts
from unique_air_quality_data
