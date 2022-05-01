{{ config(materialized='view') }}

with solar as 
(
    select *,
        row_number() over(partition by STATIONS_ID, MESS_DATUM) as row_number
        from {{ source('staging', 'solar__partitioned_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'MESS_DATUM']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y%m%d", cast(MESS_DATUM as string)) as observation_date,

    -- measurements
    {{ set_missing_values_to_null('cast(ATMO_STRAHL as float64)') }} 
                as longwave_downward_radiation,
    {{ set_missing_values_to_null('cast(FD_STRAHL as float64)') }} 
                as sum_diffuse_solar_radiation,
    {{ set_missing_values_to_null('cast(FG_STRAHL as float64)') }} 
                as sum_solar_incoming_radiation,
    {{ set_missing_values_to_null('cast(SD_STRAHL as float64)') }} 
                as sum_sunshine_duration,

from solar

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
