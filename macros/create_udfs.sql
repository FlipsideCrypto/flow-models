{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_topshot_moments_minted_metadata() }};
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
