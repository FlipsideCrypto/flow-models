{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc(object_construct('sql_source', '{{this.identifier}}','node_url','{{ var('node_url', Null) }}','external_table', 'collections', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_history']
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
        {{ ref("streamline__complete_get_collections") }}
),
collections AS (
    SELECT
        block_number AS block_height,
        DATA
    FROM
        {{ ref('streamline__complete_get_blocks') }}
        JOIN blocks
        ON blocks.block_height = block_number
),
-- CTE to get all block_heights and their associated collection_ids from the complete_get_blocks table
block_collections AS (
    SELECT
        cb.block_number AS block_height,
        collection_guarantee.value :collection_id AS collection_id
    FROM
        {{ ref("streamline__complete_get_blocks") }}
        cb,
        LATERAL FLATTEN(
            input => cb.data :collection_guarantees
        ) AS collection_guarantee
),
-- CTE to identify collections that haven't been ingested yet
collections_to_ingest AS (
    SELECT
        bc.block_height,
        bc.collection_id
    FROM
        block_collections bc
        LEFT JOIN {{ ref("streamline__complete_get_collections") }} C
        ON bc.block_height = C.block_number
        AND bc.collection_id = C.id
    WHERE
        C.id IS NULL
) -- Generate the requests based on the missing collections
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
WHERE
    block_height BETWEEN {{ var('start_block', Null) }}
    AND {{ var('end_block', Null) }}
ORDER BY
    block_height ASC