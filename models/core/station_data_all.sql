{{ config(materialized='table') }}

with st_kl_historical as (
    select *, 'historical' as station_age 
    from {{ ref('stg_station_data_kl_historical') }}
),st_kl_recent as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_kl_recent') }}
),st_soil_temp_historical as (
    select *, 'historical' as station_age 
    from {{ ref('stg_station_data_soil_temp_historical') }}
),st_soil_temp_recent as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_soil_temp_recent') }}
),st_solar_historical as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_solar') }}
),st_water_equiv_historical as (
    select *, 'historical' as station_age 
    from {{ ref('stg_station_data_water_equiv_historical') }}
),st_water_equiv_recent as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_water_equiv_recent') }}
),

station_data_unioned as (
    select * from st_kl_historical
    union all 
    select * from st_kl_recent
),

-- select * from station_data_unioned
-- where station_age like 'historical'

withoug_dup as (
    select 
        *,
        row_number() over (
            partition by station_id order by station_age desc
        ) as rank
    from station_data_unioned
)

select 
    * except(rank)
from withoug_dup
where rank = 1




