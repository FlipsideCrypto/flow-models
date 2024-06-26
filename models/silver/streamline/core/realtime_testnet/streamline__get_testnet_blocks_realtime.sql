{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc_us_east_2(object_construct('sql_source', '{{this.identifier}}','node_url','access.devnet.nodes.onflow.org:9000','external_table', 'testnet_blocks', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_realtime_testnet']
) }}
WITH post_crescendo AS (
    SELECT
        185000000 AS block_height
),
    tbl AS (
        SELECT
            block_height
        FROM
            {{ ref('streamline__testnet_blocks') }}
        WHERE
            (
                block_height >= (
                    SELECT
                        block_height
                    FROM
                        post_crescendo
                )
            )
            AND block_height IS NOT NULL
        EXCEPT
        SELECT
            block_number AS block_height
        FROM
            {{ ref('streamline__complete_get_testnet_blocks') }}
        WHERE
            (
                block_height >= (
                    SELECT
                        block_height
                    FROM
                        post_crescendo
                )
            )
            AND block_height IS NOT NULL
    )
SELECT
    OBJECT_CONSTRUCT(
        'grpc',
        'proto3',
        'method',
        'get_block_by_height',
        'block_height',
        block_height :: INTEGER,
        'method_params',
        OBJECT_CONSTRUCT(
            'height',
            block_height
        )
    ) AS request
FROM
    tbl
ORDER BY
    block_height DESC
