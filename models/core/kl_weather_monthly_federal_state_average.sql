{{ config(materialized='table') }}


select 
    observation_year,
    observation_month,
    observation_date,
    avg(wind_max_avg) as wind_max_avg,
    avg(wind_mean_avg) as wind_mean_avg,
    avg(precip_total_avg) as precip_total_avg,
    avg(sunshine_duration_avg) as sunshine_duration_avg,
    avg(snow_height_avg) as snow_height_avg,
    avg(cloud_cover_avg) as cloud_cover_avg,
    avg(vapor_pressure_avg) as vapor_pressure_avg,
    avg(temperature_2m_mean_avg) as temperature_2m_mean_avg,
    avg(relative_humidity_avg) as relative_humidity_avg,
    avg(temperature_2m_max_avg) as temperature_2m_max_avg,
    avg(temperature_2m_min_avg) as temperature_2m_min_avg,
    avg(temperature_5cm_min_avg) as temperature_5cm_min_avg,
    avg(soil_temp_002m_avg) as soil_temp_002m_avg,
    avg(soil_temp_005m_avg) as soil_temp_005m_avg,
    avg(soil_temp_010m_avg) as soil_temp_010m_avg,
    avg(soil_temp_020m_avg) as soil_temp_020m_avg,
    avg(soil_temp_050m_avg) as soil_temp_050m_avg,
    avg(longwave_downward_radiation_avg) as longwave_downward_radiation_avg,
    avg(sum_diffuse_solar_radiation_avg) as sum_diffuse_solar_radiation_avg,
    avg(sum_solar_incoming_radiation_avg) as sum_solar_incoming_radiation_avg,
    avg(sum_sunshine_duration_avg) as sum_sunshine_duration_avg,
    avg(height_of_snow_pack_sample_avg) as height_of_snow_pack_sample_avg,
    avg(total_snow_depth_avg) as total_snow_depth_avg,
    avg(total_snow_water_equivalent_avg) as total_snow_water_equivalent_avg,
    avg(sampled_snow_pack_water_eqivalent_avg) as sampled_snow_pack_water_eqivalent_avg,
    federal_state
from {{ ref('kl_weather_monthly_average') }}
group by
    federal_state,
    observation_date,
    observation_year,
    observation_month
order by 
    federal_state,
    observation_date


