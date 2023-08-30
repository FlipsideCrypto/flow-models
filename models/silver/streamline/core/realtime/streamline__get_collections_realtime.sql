{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc(object_construct('sql_source', '{{this.identifier}}','node_url','access.mainnet.nodes.onflow.org:9000','external_table', 'collections', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_realtime']
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}
    SELECT
        {# 55114467 is start of mainnet 23 #}
        55114467 AS block_height
{% else %}

    SELECT
        MAX(block_height) - 210000 AS block_height
    FROM
        {{ ref('streamline__blocks') }}
    {% endif %}),
    tbl AS (
        SELECT
            block_height
        FROM
            {{ ref('streamline__blocks') }}
        WHERE
            (
                block_height >= (
                    SELECT
                        block_height
                    FROM
                        last_3_days
                )
            )
            AND block_height IS NOT NULL
        EXCEPT
        SELECT
            block_number AS block_height
        FROM
            {{ ref('streamline__complete_get_collections') }}
        WHERE
            (
                block_height >= (
                    SELECT
                        block_height
                    FROM
                        last_3_days
                )
            )
            AND block_height IS NOT NULL
    ),
    collections AS (
    SELECT
        block_number AS block_height,
        DATA
    FROM
        {{ ref('streamline__complete_get_blocks') }}
        JOIN tbl
        ON tbl.block_height = block_number
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
)
SELECT
    OBJECT_CONSTRUCT(
        'grpc', 'proto3',
        'method', 'get_collection_by_i_d',
        'block_height', block_height :: INTEGER,
        'method_params', OBJECT_CONSTRUCT(
            'id',
            collection_id
        )
    ) AS request
FROM
    collections_to_ingest
ORDER BY
    block_height ASC
{# LIMIT
    250000 #}
