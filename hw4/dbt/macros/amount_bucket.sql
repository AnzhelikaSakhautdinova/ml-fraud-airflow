{% macro amount_bucket(column) %}
    case
        when {{ column }} < 100 then 'SMALL'
        when {{ column }} < 1000 then 'MEDIUM'
        else 'LARGE'
    end
{% endmacro %}
