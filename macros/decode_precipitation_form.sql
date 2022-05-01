{#
    This macro decodes the precipitation form of the kl weather data.
#}

{% macro decode_precipitation_form(precip_code) -%}

    case {{ precip_code }}
        when 0 then 'No precipitation'
        when 1 then 'Only rain'
        when 4 then 'Unknown precipitation'
        when 6 then 'Only rain'
        when 7 then 'Only snow'
        when 8 then 'Rain and snow'
        when 9 then 'Error or missing value'
    end

{%- endmacro %}