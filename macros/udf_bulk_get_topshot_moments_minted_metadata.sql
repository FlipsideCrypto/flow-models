{% macro udf_bulk_get_topshot_moments_minted_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_topshot_moments_minted_metadata() returns text api_integration = aws_flow_api_dev AS {% if target.name == "prod" -%}
        'https://3ltti6kisi.execute-api.us-east-1.amazonaws.com/prod/bulk_get_topshot_moments_minted_metadata'
    {% else %}
        'https://wn6lmi2rs4.execute-api.us-east-1.amazonaws.com/dev/bulk_get_topshot_moments_minted_metadata'
    {%- endif %}
{% endmacro %}