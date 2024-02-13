{% macro create_udf_get_chainhead() %}    
    {{ log("Creating udf get_chainhead for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = 
    {% if target.name == "prod" %} 
        aws_flow_api_prod AS 'https://quxfxtl934.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% elif target.name == "dev" %}
        aws_flow_api_dev_2 AS 'https://ul6x832e8l.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx get_chainhead", info=True) }}
        aws_flow_api_sbx AS 'https://bc5ejedoq8.execute-api.us-east-1.amazonaws.com/sbx/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_grpc() %}    
    {{ log("Creating udf udf_bulk_grpc for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_grpc(json variant) returns variant api_integration = 
    {% if target.name == "prod" %} 
        aws_flow_api_prod AS 'https://quxfxtl934.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_grpc'
    {% elif target.name == "dev" %}
        aws_flow_api_dev_2 AS 'https://ul6x832e8l.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_grpc'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx udf_bulk_grpc", info=True) }}
        aws_flow_api_sbx AS 'https://bc5ejedoq8.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_grpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_grpc_us_east_2() %}    
    {{ log("Creating udf udf_bulk_grpc for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_grpc_us_east_2(json variant) returns variant api_integration = 
    {% if target.name == "prod" %} 
        aws_flow_api_prod_us_east_2 AS 'https://78rpbojpue.execute-api.us-east-2.amazonaws.com/prod/udf_bulk_grpc'
    {% elif target.name == "dev" %}
        aws_flow_api_dev_3 AS 'https://j6qalrfe69.execute-api.us-east-2.amazonaws.com/dev/udf_bulk_grpc'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx udf_bulk_grpc", info=True) }}
        aws_flow_api_sbx AS 'https://bc5ejedoq8.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_grpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_api() %}    
    {{ log("Creating udf udf_bulk_grpc for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_api(
        method VARCHAR,
        url VARCHAR,
        headers OBJECT,
        DATA OBJECT,
        user_id VARCHAR,
        secret_name VARCHAR
    ) returns variant api_integration = 
    {% if target.name == "prod" %} 
        aws_flow_api_prod AS 'https://quxfxtl934.execute-api.us-east-1.amazonaws.com/prod/udf_api'
    {% elif target.name == "dev" %}
        aws_flow_api_dev_2 AS 'https://ul6x832e8l.execute-api.us-east-1.amazonaws.com/dev/udf_api'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx udf_api", info=True) }}
        aws_flow_api_sbx AS 'https://bc5ejedoq8.execute-api.us-east-1.amazonaws.com/sbx/udf_api'
    {%- endif %};
{% endmacro %}

