{{ config(materialized='table') }}

with weather_all as (
    select * 
    from {{ ref('kl_weather_all') }}
),
station_data as (
    select *
    from {{ ref('station_data_all') }}
)

select 
    station_id,
    * except(station_id_kl, station_id)
from (
    select 
        station_id as station_id_kl,
        concat(extract(year from observation_date)) as observation_year,
        extract(month from observation_date) as observation_month,
        parse_date("%Y%m", concat(extract(year from observation_date),
                                  extract(month from observation_date))) 
                   as observation_date,
        avg(wind_max) as wind_max_avg,
        avg(wind_mean) as wind_mean_avg,
        avg(precip_total) as precip_total_avg,
        avg(sunshine_duration) as sunshine_duration_avg,
        avg(snow_height) as snow_height_avg,
        avg(cloud_cover) as cloud_cover_avg,
        avg(vapor_pressure) as vapor_pressure_avg,
        avg(temperature_2m_mean) as temperature_2m_mean_avg,
        avg(relative_humidity) as relative_humidity_avg,
        avg(temperature_2m_max) as temperature_2m_max_avg,
        avg(temperature_2m_min) as temperature_2m_min_avg,
        avg(temperature_5cm_min) as temperature_5cm_min_avg,
        avg(soil_temp_002m) as soil_temp_002m_avg,
        avg(soil_temp_005m) as soil_temp_005m_avg,
        avg(soil_temp_010m) as soil_temp_010m_avg,
        avg(soil_temp_020m) as soil_temp_020m_avg,
        avg(soil_temp_050m) as soil_temp_050m_avg,
        avg(longwave_downward_radiation) as longwave_downward_radiation_avg,
        avg(sum_diffuse_solar_radiation) as sum_diffuse_solar_radiation_avg,
        avg(sum_solar_incoming_radiation) as sum_solar_incoming_radiation_avg,
        avg(sum_sunshine_duration) as sum_sunshine_duration_avg,
        avg(height_of_snow_pack_sample) as height_of_snow_pack_sample_avg,
        avg(total_snow_depth) as total_snow_depth_avg,
        avg(total_snow_water_equivalent) as total_snow_water_equivalent_avg,
        avg(sampled_snow_pack_water_eqivalent) as sampled_snow_pack_water_eqivalent_avg,
    from 
        weather_all
    where 
        parse_date("%Y%m", concat(extract(year from observation_date),
                                  extract(month from observation_date)))
                < parse_date("%Y%m", concat(extract(year from CURRENT_DATE()),
                                  extract(month from CURRENT_DATE())))
    group by
        station_id, observation_year, observation_month, observation_date
) as weather_all_avg
join station_data
    on weather_all_avg.station_id_kl = station_data.station_id
order by 
    station_id, observation_date






