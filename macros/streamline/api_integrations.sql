-- macro used to create flow api integrations
{% macro create_aws_flow_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/flow-api-prod-rolesnowflakeudfsAF733095-FNY67ODG1RFG' api_allowed_prefixes = (
            'https://quxfxtl934.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}

        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_prod_us_east_2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/flow-api-prod-rolesnowflakeudfsAF733095-F6SPYWFGQX9Z' api_allowed_prefixes = (
            'https://78rpbojpue.execute-api.us-east-2.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
        
    {% elif target.name == "dev" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_dev_2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/flow-api-dev-rolesnowflakeudfsAF733095-1IP9GV997U5RM' api_allowed_prefixes = (
            'https://ul6x832e8l.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;    
        {% endset %}
        {% do run_query(sql) %}
        
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_flow_api_dev_3 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/flow-api-dev-rolesnowflakeudfsAF733095-Q0LF66KP892M' api_allowed_prefixes = (
            'https://j6qalrfe69.execute-api.us-east-2.amazonaws.com/dev/'
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
