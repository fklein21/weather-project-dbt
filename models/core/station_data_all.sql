{{ config(materialized='table') }}

with st_kl_historical as (
    select *, 'historical' as station_age 
    from {{ ref('stg_station_data_kl_historical') }}
),
st_kl_recent as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_kl_recent') }}
),
st_soil_temp_historical as (
    select *, 'historical' as station_age 
    from {{ ref('stg_station_data_soil_temp_historical') }}
),
st_soil_temp_recent as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_soil_temp_recent') }}
),
st_solar as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_solar') }}
),
st_water_equiv_historical as (
    select *, 'historical' as station_age 
    from {{ ref('stg_station_data_water_equiv_historical') }}
),
st_water_equiv_recent as (
    select *, 'recent' as station_age 
    from {{ ref('stg_station_data_water_equiv_recent') }}
),
st_phenology as (
    select *, 'recent' as station_age
    from {{ ref('stg_station_data_phenology') }}
),


station_data_unioned as (
    select * from st_kl_historical
    union all 
    select * from st_kl_recent
    union all 
    select * from st_soil_temp_historical
    union all 
    select * from st_soil_temp_recent
    union all 
    select * from st_water_equiv_historical
    union all 
    select * from st_water_equiv_recent
    union all 
    select * from st_solar
    union all 
    select * from st_phenology
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
order by station_id

-- select * from st_solar
-- select * from station_data_unioned


