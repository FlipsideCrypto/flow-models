{% macro generate_blocks_grpc_request(block_height) %}    
    PARSE_JSON(
        CONCAT(
            '{"grpc": "proto3",',
            '"method": "get_block_by_height",',
            '"block_height":"',
            block_height :: INTEGER,
            '",',
            '"method_params": {"height":',
            block_height :: INTEGER,
            '}}'
        )
    )
{% endmacro %}

{% macro generate_collections_grpc_request(block_height, collection_guarantee) %}    
    PARSE_JSON(
        CONCAT(
            '{"grpc": "proto3",',
            '"method": "get_collection_by_i_d",',
            '"block_height":"',
            block_height :: INTEGER,
            '",',
            '"method_params": {"id":"',
            collection_guarantee.value:collection_id,
            '"}}'
        )
    )
{% endmacro %}


{% macro config_core_utils(schema="utils") %}


- name: {{ schema }}.udf_register_secret
  signature:
    - [request_id, STRING]
    - [key, STRING]
  func_type: SECURE
  return_type: TEXT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    IMMUTABLE
  sql: |
    SELECT
      _utils.UDF_REGISTER_SECRET(REQUEST_ID, _utils.UDF_WHOAMI(), KEY)

{% endmacro %}


{% macro if_data_call_function_v2(
        func,
        target,
        params
    ) %}
    {% if var(
            "STREAMLINE_INVOKE_STREAMS"
        ) %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: Calling udf " ~ func ~ " with params: \n" ~ params | tojson(indent=2) ~  "\n on " ~ target,
                True
            ) }}
        {% endif %}
    SELECT
        {{ func }}( parse_json($${{ params | tojson }}$$) )
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ target }}
            LIMIT
                1
        )
    {% else %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: NOOP",
                False
            ) }}
        {% endif %}
    SELECT
        NULL
    {% endif %}
{% endmacro %}