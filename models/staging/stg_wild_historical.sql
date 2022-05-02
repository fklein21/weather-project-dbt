{{ config(materialized='view') }}


with wild_historical as
(
    select 
        cast(STATIONS_ID as int) as STATIONS_ID,
        cast(REFERENZJAHR as int) as REFERENZJAHR,
        cast(QUALITAETSNIVEAU as int) as QUALITAETSNIVEAU,
        cast(OBJEKT_ID as int) as OBJEKT_ID,
        cast(PHASE_ID as int) as PHASE_ID,
        EINTRITTSDATUM,
        cast(EINTRITTSDATUM_QB as int) as EINTRITTSDATUM_QB,
        cast(JULTAG as int) as JULTAG,
    from {{ source('staging', 'wild_historical_partitioned_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'REFERENZJAHR']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y", cast(REFERENZJAHR as string)) as observation_year_date,
    REFERENZJAHR as observation_year,

    -- observation
    OBJEKT_ID as plant_id,
    PHASE_ID as phase_id,
    parse_date("%Y%m%d", cast(EINTRITTSDATUM as string)) as observation_date,
    extract(dayofyear from parse_date("%Y%m%d", cast(EINTRITTSDATUM as string))) 
                as observation_day_of_year,
    JULTAG as jultag

from wild_historical


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
