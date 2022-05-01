{{ config(materialized='view') }}

with soil_temp_recent as 
(
    select *,
        row_number() over(partition by STATIONS_ID, MESS_DATUM) as row_number
        from {{ source('staging', 'soil_temperature_recent_partitioned_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'MESS_DATUM']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y%m%d", cast(MESS_DATUM as string)) as observation_date,


    -- measurements
    {{ set_missing_values_to_null('V_TE002M') }} as soil_temp_002m, 
    {{ set_missing_values_to_null('V_TE005M') }} as soil_temp_005m, 
    {{ set_missing_values_to_null('V_TE010M') }} as soil_temp_010m, 
    {{ set_missing_values_to_null('V_TE020M') }} as soil_temp_020m, 
    {{ set_missing_values_to_null('V_TE050M') }} as soil_temp_050m

from soil_temp_recent


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
