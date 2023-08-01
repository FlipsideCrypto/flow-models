{% macro create_udf_get_chainhead() %}    
    {{ log("Creating udf get_chainhead for target:" ~ target.name ~ ", schema: " ~ target.schema, info=True) }}
    {{ log("role:" ~ target.role ~ ", user:" ~ target.user, info=True) }}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = 
    {% if target.name == "prod" %} 
        aws_flow_api AS '<PROD_CHALICE_URL>/prod/get_chainhead'
    {% elif target.name == "dev" %}
        aws_flow_api_dev AS '<DEV_CHALICE_URL>/dev/get_chainhead'
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
        aws_flow_api AS '<PROD_CHALICE_URL>/prod/udf_bulk_grpc'
    {% elif target.name == "dev" %}
        aws_flow_api_dev AS '<DEV_CHALICE_URL>/dev/udf_bulk_grpc'
    {% elif  target.name == "sbx" %}
        {{ log("Creating sbx udf_bulk_grpc", info=True) }}
        aws_flow_api_sbx AS 'https://bc5ejedoq8.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_grpc'
    {%- endif %};
{% endmacro %}

