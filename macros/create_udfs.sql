{% macro create_udfs() %}
    {% if target.database != "FLOW_COMMUNITY_DEV" %}
        {% set sql %}
        {{ udf_bulk_get_topshot_moments_minted_metadata() }};
        {{ udf_bulk_get_nfl_allday_moments_metadata() }};
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
