{% macro udf_bulk_get_nfl_allday_moments_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_nfl_allday_moments_metadata() returns text api_integration = aws_flow_api_dev AS {% if target.database == "FLOW" -%}
        'https://3ltti6kisi.execute-api.us-east-1.amazonaws.com/prod/bulk_get_nfl_allday_metadata'
    {% else %}
        'https://wn6lmi2rs4.execute-api.us-east-1.amazonaws.com/dev/bulk_get_nfl_allday_metadata'
    {%- endif %}
{% endmacro %}
