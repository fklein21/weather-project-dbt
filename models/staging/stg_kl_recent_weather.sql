{{ config(materialized='view') }}

with weather_recent as 
(
    select *,
        row_number() over(partition by STATIONS_ID, MESS_DATUM) as row_number
        from {{ source('staging', 'kl_recent_partitioned_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'MESS_DATUM']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y%m%d", cast(MESS_DATUM as string)) as observation_date,

    -- measurements
    {{ set_missing_values_to_null('FX') }} as wind_max,
    {{ set_missing_values_to_null('FM') }} as wind_mean, 
    {{ set_missing_values_to_null('RSK') }} as precip_total,
    {{ set_missing_values_to_null('RSKF') }} as precip_category_raw,
    {{ decode_precipitation_form('RSKF') }} as precip_category,
    {{ set_missing_values_to_null('SDK') }} as sunshine_duration,
    {{ set_missing_values_to_null('SHK_TAG') }} as snow_height,
    {{ set_missing_values_to_null('NM') }} as cloud_cover,
    {{ set_missing_values_to_null('VPM') }} as vapor_pressure,
    {{ set_missing_values_to_null('TMK') }} as temperature_2m_mean,
    {{ set_missing_values_to_null('UPM') }} as relative_humidity,
    {{ set_missing_values_to_null('TXK') }} as temperature_2m_max,
    {{ set_missing_values_to_null('TNK') }} as temperature_2m_min,
    {{ set_missing_values_to_null('TGK') }} as temperature_5cm_min

from weather_recent

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
