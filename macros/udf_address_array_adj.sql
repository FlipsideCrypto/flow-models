{% macro run_create_address_array_adj() %}
{% set sql %}
create or replace function {{ target.database }}.silver.udf_address_array_adj(address_list ARRAY) 
returns array
language python
runtime_version = '3.9'
handler = 'address_array_adj'
AS
$$
def address_array_adj(addresses):
    return ["0x" + addr for addr in addresses]
$$
;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
