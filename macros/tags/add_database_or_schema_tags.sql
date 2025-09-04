{% macro add_database_or_schema_tags() %}
    {{ set_database_tag_value(
        'BLOCKCHAIN_NAME',
        'FLOW'
    ) }}
    {{ set_schema_tag_value(
        'CORE_EVM',
        'BLOCKCHAIN_NAME',
        'FLOW-EVM'
    ) }}
{% endmacro %}
