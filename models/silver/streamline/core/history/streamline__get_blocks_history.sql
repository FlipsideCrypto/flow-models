{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc(object_construct('sql_source', '{{this.identifier}}','method', 'get_block_by_height', 'external_table', 'streamline_blocks', 'sql_limit', {{var('sql_limit','1000')}}, 'producer_batch_size', {{var('producer_batch_size','1000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_height,
        node_url
    FROM
        {{ ref("streamline__blocks") }}
    -- EXCEPT
    -- SELECT
    --     block_id as block_height,
    --     node_url
    -- FROM
    --     {{ ref("streamline__complete_get_blocks") }}
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"grpc": "proto3",',
            '"method": "get_block_by_height",',
            '"block_height":"',
            block_height :: INTEGER,
            '"}'
        )
    ) AS request,
    node_url
FROM
    blocks
ORDER BY
    block_height ASC
