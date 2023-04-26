{% macro get_flow_txs_api() %}
{% set sql %}

CALL {{ target.database }}.bronze_api.flow_txs_api()

{% endset %}
{% do run_query(sql) %}
{% endmacro%}
