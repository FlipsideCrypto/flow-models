{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% if target.database != "FLOW_COMMUNITY_DEV" %}
            {% set sql %}
            
            {{ udf_bulk_get_topshot_moments_minted_metadata() }};
            {{ udf_bulk_get_nfl_allday_moments_metadata() }};

            {{ create_udtf_get_base_table(
                schema = "streamline"
            ) }}
            {{ create_udf_get_chainhead() }}
            {{ create_udf_bulk_grpc() }}
            {{ create_udf_get_node_url() }}
            
            {% endset %}
            {% do run_query(sql) %}
            {{- fsc_utils.create_udfs() -}}
        {% endif %}
    {% endif %}
{% endmacro %}
