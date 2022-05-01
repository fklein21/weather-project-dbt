{{ config(materialized='view') }}

with water_equiv_historical as 
(
    select *,
        row_number() over(partition by STATIONS_ID, MESS_DATUM) as row_number
        from {{ source('staging', 'water_equiv_historical_partitioned_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'MESS_DATUM']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y%m%d", cast(MESS_DATUM as string)) as observation_date,

    -- measurements
    {{ set_missing_values_to_null('cast(ASH_6 as float64)') }}  
                                as height_of_snow_pack_sample,
    {{ set_missing_values_to_null('cast(SH_TAG as float64)') }}  
                                as total_snow_depth, 
    {{ set_missing_values_to_null('cast(WASH_6 as float64)') }}  
                                as total_snow_water_equivalent,
    {{ set_missing_values_to_null('cast(WAAS_6 as float64)') }}  
                                as sampled_snow_pack_water_eqivalent,

from water_equiv_historical


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
