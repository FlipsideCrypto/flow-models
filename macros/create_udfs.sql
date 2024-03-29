{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% if target.database != "FLOW_COMMUNITY_DEV" %}
            {% set sql %}

            {{ create_udtf_get_base_table(
                schema = "streamline"
            ) }}
            {{ create_udf_get_chainhead() }}
            {{ create_udf_bulk_grpc() }}
            
            {{ run_create_udf_array_disjunctive_union() }}
            {{ run_create_address_array_adj() }}
            
            {% endset %}
            {% do run_query(sql) %}
            {{- fsc_utils.create_udfs() -}}
        {% endif %}
    {% endif %}
{% endmacro %}
