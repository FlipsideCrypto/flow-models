{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc_us_east_2(object_construct('sql_source', '{{this.identifier}}','node_url','access-001.mainnet5.nodes.onflow.org:9000','external_table', 'blocks_mainnet_05', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_height
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number AS block_height
    FROM
        {{ ref("streamline__complete_get_blocks") }}
)
SELECT
    OBJECT_CONSTRUCT(
        'grpc', 'proto3',
        'method', 'get_block_by_height',
        'block_height', block_height,
        'method_params', OBJECT_CONSTRUCT('height', block_height)
    ) AS request
FROM
    blocks
WHERE
    block_height BETWEEN 12020337
    AND 12609236
ORDER BY
    block_height ASC
