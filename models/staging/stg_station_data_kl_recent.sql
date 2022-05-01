{{ config(materialized='view') }}


select

    cast(Stations_id as int) as station_id,
    parse_date("%Y%m%d", cast(von_datum as string)) as date_from,
    parse_date("%Y%m%d", cast(bis_datum as string)) as date_to,
    cast(Stationshoehe as int) as altitude,
    cast(geoBreite as float64) as latitude,
    cast(geoLaenge as float64) as longitude,
    cast(Stationsname as string) as station_name,
    cast(Bundesland as string) as federal_state,

from {{ source('staging', 'kl_recent_station_data_internal_table')}}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
