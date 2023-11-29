-- macros/check_table_exists.sql
{% macro check_table_exists(schema_name, table_name) %}
  {% set query %}
        SELECT count(*)
        FROM information_schema.tables 
        WHERE table_schema = '{{ schema_name }}'
        AND table_name = '{{ table_name }}'
  {% endset %}

  {% set results = run_query(query) %}
  {% if execute %}
    {% if results and results.rows[0][0] > 0 %}
      "True"
    {% else %}
      "False"
    {% endif %}
  {% endif %}
{% endmacro %}
