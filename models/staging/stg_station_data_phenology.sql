{{ config(materialized='view') }}


select
    cast(Stations_id as int) as station_id,
    parse_date("%d.%m.%Y", cast(Datum_Stationsaufloesung as string)) as date_from,
    parse_date("%d.%m.%Y", cast(Datum_Stationsaufloesung as string)) as date_to,
    cast(Stationshoehe as int) as altitude,
    cast(geograph_Breite as float64) as latitude,
    cast(geograph_Laenge as float64) as longitude,
    cast(Stationsname as string) as station_name,
    cast(Bundesland as string) as federal_state,

from {{ ref('PH_Beschreibung_Phaenologie_Stationen_Sofortmelder')}}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
