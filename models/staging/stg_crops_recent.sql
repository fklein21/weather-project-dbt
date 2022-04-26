{{ config(materialized='view') }}


with crops_recent as
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
    from {{ source('staging', 'crops_recent_partitioned_table')}}
    where 
        not ltrim(rtrim(cast(EINTRITTSDATUM as string))) = ''
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['STATIONS_ID', 'REFERENZJAHR']) }} as mid,
    STATIONS_ID as station_id,
    -- observation date
    --parse_date(cast(MESS_DATUM as string), "%Y%m%d") as mdate
    parse_date("%Y", cast(REFERENZJAHR as string)) as observation_year,

    -- observation
    OBJEKT_ID as plant_type,
    PHASE_ID as phase_id,
    parse_date("%Y%m%d", cast(EINTRITTSDATUM as string)) as observation_date,
    JULTAG as jultag

from crops_recent


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
