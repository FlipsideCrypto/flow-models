-- macro used to create flow api integrations
{% macro create_aws_flow_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-flow' api_allowed_prefixes = (
            'https://<PROD_FLOW_API_CHALICE_URL>/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "dev" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_dev_2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/flow-api-dev-rolesnowflakeudfsAF733095-1D0U05G1EDT3' api_allowed_prefixes = (
            'https://8jjulyhxhj.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "sbx" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_sbx api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::579011195466:role/flow-api-sbx-rolesnowflakeudfsAF733095-1R9BM6QXEKD5O' api_allowed_prefixes = (
            'https://bc5ejedoq8.execute-api.us-east-1.amazonaws.com/sbx'
        ) enabled = TRUE;
        {% endset %}
        -- {% do run_query(sql) %}
        {% set query_result = run_and_log_sql(sql) %}
    {% endif %}
{% endmacro %}
