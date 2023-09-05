{% macro run_create_udf_array_disjunctive_union() %}
    {% set func_sql %}
    CREATE
    OR REPLACE FUNCTION {{ target.database }}.silver.udf_array_disjunctive_union(
        a1 ARRAY,
        a2 ARRAY
    ) returns ARRAY LANGUAGE javascript AS 'return [...A1.filter(e => !A2.includes(e)),...A2.filter(e => !A1.includes(e))]';
{% endset %}
    {% do run_query(func_sql) %}
{% endmacro %}
