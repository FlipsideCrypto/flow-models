{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc_v2(object_construct('sql_source', '{{this.identifier}}','node_url','access.devnet.nodes.onflow.org:9000','external_table', 'testnet_collections_v2', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_realtime_testnet']
) }}

WITH 
min_block_height AS (
    SELECT
        279500000 AS block_height
),
    -- CTE to get targeted block_heights and their associated collection_ids from the complete_get_blocks table
    block_collections AS (
        SELECT
            cb.block_number AS block_height,
            collection_guarantee.value :collection_id AS collection_id
        FROM
            {{ ref("streamline__complete_get_testnet_blocks") }}
            cb,
            LATERAL FLATTEN(
                input => cb.data :collection_guarantees
            ) AS collection_guarantee
        WHERE
            block_height >= (
                SELECT
                    block_height
                FROM
                    min_block_height
            )
    ),
    -- CTE to identify collections that haven't been ingested yet
    collections_to_ingest AS (
        SELECT
            bc.block_height,
            bc.collection_id
        FROM
            block_collections bc
            LEFT JOIN {{ ref("streamline__complete_get_testnet_collections") }} C
            ON bc.block_height = C.block_number
            AND bc.collection_id = C.id
        WHERE
            C.id IS NULL
    )
SELECT
    OBJECT_CONSTRUCT(
        'grpc',
        'proto3',
        'method',
        'get_collection_by_i_d',
        'block_height',
        block_height :: INTEGER,
        'method_params',
        OBJECT_CONSTRUCT(
            'id',
            collection_id
        )
    ) AS request
FROM
    collections_to_ingest
ORDER BY
    block_height DESC
