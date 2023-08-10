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