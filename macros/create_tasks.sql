{% macro create_tasks() %}
    {% if target.database == 'FLOW' %}
        CREATE SCHEMA IF NOT EXISTS _internal;
        {{ task_run_sp_create_prod_clone('_internal') }};
    {% endif %}

{% endmacro %}