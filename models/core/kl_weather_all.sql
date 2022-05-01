{{ config(materialized='table') }}

-- KL weather data ----------------------------------------------
with kl_recent as (
    select 
        *,
        'kl_recent' as observation_type_kl
    from {{ ref('stg_kl_recent_weather') }}
),

kl_historical as (
    select
        *,
        'kl_historical' as observation_type_kl
    from {{ ref('stg_kl_historical_weather') }}
),

kl_obs_unioned as (
    select * from kl_recent
    union all
    select * from kl_historical
),

kl_without_dup as (
    select *,
    row_number() over (partition by mid order by observation_type_kl asc) as rank
    from kl_obs_unioned
), 

-- soil temperature data ----------------------------------------
soil_temp_recent as (
    select 
        *,
        'soil_temp_recent' as observation_type_soil_temp
    from {{ ref('stg_soil_temp_recent') }}
),

soil_temp_historical as (
    select
        *,
        'soil_temp_historical' as observation_type_soil_temp
    from {{ ref('stg_soil_temp_historical') }}
),

soil_temp_obs_unioned as (
    select * from soil_temp_recent
    union all
    select * from soil_temp_historical
),

soil_temp_without_dup as (
    select *,
    row_number() over (partition by mid order by observation_type_soil_temp asc) as rank
    from soil_temp_obs_unioned
), 


-- solar data ---------------------------------------------------
solar_without_dup as (
    select 
        *,
        'solar' as observation_type_solar
    from {{ ref('stg_solar') }}
),

-- -- water equiv data ---------------------------------------------
water_equiv_recent as (
    select 
        *,
        'water_equiv_recent' as observation_type_water_equiv
    from {{ ref('stg_water_equiv_recent') }}
),

water_equiv_historical as (
    select
        *,
        'water_equiv_historical' as observation_type_water_equiv
    from {{ ref('stg_water_equiv_historical') }}
),

water_equiv_obs_unioned as (
    select * from water_equiv_recent
    union all
    select * from water_equiv_historical
),

water_equiv_without_dup as (
    select *,
    row_number() over (partition by mid order by observation_type_water_equiv asc) as rank
    from water_equiv_obs_unioned
),


station_data as (
    select *
    from {{ ref('station_data_all') }}
)


select 
    kl_data.mid,
    kl_data.station_id as station_id,
    kl_data.observation_date as observation_date,
    * except(rank, station_id, mid, observation_date)
from kl_without_dup as kl_data
left join soil_temp_without_dup as soil_temp_data
    on kl_data.station_id = soil_temp_data.station_id
    and kl_data.observation_date = soil_temp_data.observation_date
    and kl_data.rank = soil_temp_data.rank
left join solar_without_dup as solar_data
    on kl_data.station_id = solar_data.station_id
    and kl_data.observation_date = solar_data.observation_date
left join water_equiv_without_dup as water_equiv_data
    on kl_data.station_id = water_equiv_data.station_id
    and kl_data.observation_date = water_equiv_data.observation_date
    and kl_data.rank = water_equiv_data.rank
left join station_data
    on kl_data.station_id = station_data.station_id
where kl_data.rank = 1


