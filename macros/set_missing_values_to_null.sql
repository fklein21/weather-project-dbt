{#
    This macro sets missing values (-999) to NULL
#}

{% macro set_missing_values_to_null(measurement) -%}

    case {{ measurement }}
        when -999 then NULL
        else {{ measurement }}
    end

{%- endmacro %}