{{ config(materialized='view') }}

with weather_historical as 
(
    select *,
        row_number() over(partition by STATIONS_ID, MESS_DATUM) as row_number
        from {{ source('staging', 'kl_historical_partitioned_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'MESS_DATUM']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y%m%d", cast(MESS_DATUM as string)) as observation_date,

    -- measurements
    FX as wind_max,
    FM as wind_mean, 
    RSK as precip_total,
    RSKF as precip_category,
    SDK as sunshine_duration,
    SHK_TAG as snow_height,
    NM as cloud_cover,
    VPM as vapor_pressure,
    TMK as temperature_2m_mean,
    UPM as relative_humidity,
    TXK as temperature_2m_max,
    TNK as temperature_2m_min,
    TGK as temperature_5cm_min

from weather_historical


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
