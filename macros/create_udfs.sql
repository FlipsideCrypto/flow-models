{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
            {% set sql %}

            {{ create_udtf_get_base_table(
                schema = "streamline"
            ) }}
            {{ create_udf_get_chainhead() }}
            {{ create_udf_get_chainhead_testnet() }}
            {{ create_udf_bulk_grpc_v2() }}
            
            {{ run_create_udf_array_disjunctive_union() }}
            {{ run_create_address_array_adj() }}
            
            {% endset %}
            {% do run_query(sql) %}
            {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}
