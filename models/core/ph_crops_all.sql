{{ config(materialized='table') }}

-- crops data ----------------------------------------------
with crops_recent as (
    select 
        *,
        'crops_recent' as observation_type_crops,
        'crops' as plant_type
    from {{ ref('stg_crops_recent') }}
),

crops_historical as (
    select
        *,
        'crops_historical' as observation_type_crops,
        'crops' as plant_type
    from {{ ref('stg_crops_historical') }}
),

crops_unioned as (
    select * from crops_recent
    union all
    select * from crops_historical
),

crops_without_dup as (
    select *,
    row_number() over (partition by station_id, observation_year 
                       order by observation_type_crops asc) as rank
    from crops_unioned
),

-- fruit data ----------------------------------------------
fruit_recent as (
    select 
        *,
        'fruit_recent' as observation_type_fruit,
        'fruit' as plant_type
    from {{ ref('stg_fruit_recent') }}
),

fruit_historical as (
    select
        *,
        'fruit_historical' as observation_type_fruit,
        'fruit' as plant_type
    from {{ ref('stg_fruit_historical') }}
),

fruit_unioned as (
    select * from fruit_recent
    union all
    select * from fruit_historical
),

fruit_without_dup as (
    select *,
    row_number() over (partition by station_id, observation_year 
                       order by observation_type_fruit asc) as rank
    from fruit_unioned
),

-- wild data ----------------------------------------------
wild_recent as (
    select 
        *,
        'wild_recent' as observation_type_wild,
        'wild' as plant_type
    from {{ ref('stg_wild_recent') }}
),

wild_historical as (
    select
        *,
        'wild_historical' as observation_type_wild,
        'wild' as plant_type
    from {{ ref('stg_wild_historical') }}
),

wild_unioned as (
    select * from wild_recent
    union all
    select * from wild_historical
),

wild_without_dup as (
    select *,
    row_number() over (partition by station_id, observation_year 
                       order by observation_type_wild asc) as rank
    from wild_unioned
),

station_data as (
    select *
    from {{ ref('station_data_all') }}
),

phases as (
    select *
    from {{ ref('stg_ph_all_phases') }}
)

select 
crops_data.station_id,
station_name,
plant_type,
crops_data.plant_id,
plantname_ger,
plantname_en,
plantname_latin,
observation_year_date,
observation_year,
crops_data.phase_id,
phase,
phase_en,
phase_definition,
observation_date,
observation_day_of_year,
jultag,
bbch_code,
bbch_note,
date_from,
date_to,
altitude,
latitude,
longitude,
federal_state,
station_age
from crops_without_dup as crops_data
left join station_data
    on crops_data.station_id = station_data.station_id
left join phases
    on crops_data.plant_id = phases.plant_id and
       crops_data.phase_id = phases.phase_id
where crops_data.rank = 1
order by crops_data.station_id,
         crops_data.plant_id,
         crops_data.phase_id






