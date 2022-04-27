
merge {{ ref('stg_kl_recent_weather') }}  as target
using {{ ref('stg_kl_historical_weather') }} as source
on weather_recent = weather_historical.mid
when matched then
  update set 
    target.mid = source.mid,
    target.station_id = source.station_id,
    target.observation_date = source.observation_date,
    target.wind_max = source.wind_max,
    target.wind_mean = source.wind_mean, 
    target.precip_total = source.precip_total,
    target.precip_category = source.precip_category,
    target.sunshine_duration = source.sunshine_duration,
    target.snow_height = source.snow_height,
    target.cloud_cover = source.cloud_cover,
    target.vapor_pressure = source.vapor_pressure,
    target.temperature_2m_mean = source.temperature_2m_mean,
    target.relative_humidity = source.relative_humidity,
    target.temperature_2m_max = source.temperature_2m_max,
    target.temperature_2m_min = source.temperature_2m_min,
    target.temperature_5cm_min = source.temperature_5cm_min

when not matched then
  insert (
    mid,
    station_id,
    observation_date,
    wind_max,
    wind_mean,
    precip_total,
    precip_category,
    sunshine_duration,
    snow_height,
    cloud_cover,
    vapor_pressure,
    temperature_2m_mean,
    relative_humidity,
    temperature_2m_max,
    temperature_2m_min,
    temperature_5cm_min
  )
  values (
    source.mid,
    source.station_id,
    source.observation_date,
    source.wind_max,
    source.wind_mean,
    source.precip_total,
    source.precip_category,
    source.sunshine_duration,
    source.snow_height,
    source.cloud_cover,
    source.vapor_pressure,
    source.temperature_2m_mean,
    source.relative_humidity,
    source.temperature_2m_max,
    source.temperature_2m_min,
    source.temperature_5cm_min,
  )