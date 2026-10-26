{# ClickHouse dispatch for the pinned dbt_date macro used in staging. #}
{% macro clickhouse__day_of_week(date, isoweek=true) %}
    {% if isoweek %}
        toDayOfWeek({{ date }})
    {% else %}
        modulo(toDayOfWeek({{ date }}), 7) + 1
    {% endif %}
{% endmacro %}
