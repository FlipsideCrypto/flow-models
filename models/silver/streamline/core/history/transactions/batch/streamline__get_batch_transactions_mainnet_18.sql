{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'udf_bulk_grpc',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "node_url":"access-001.mainnet18.nodes.onflow.org:9000",
            "external_table": "transaction_mainnet_18",
            "sql_limit": "1000",
            "producer_batch_size": "100",
            "worker_batch_size": "10",
            "sql_source": "{{this.identifier}}",
            "concurrent_requests": "750"
        }
    )
) }}

WITH collection_transactions AS (

    SELECT
        block_number AS block_height,
        TRANSACTION.value :: STRING AS transaction_id
    FROM
        {{ ref('streamline__complete_get_collections_history') }}
        cch,
        LATERAL FLATTEN(
            input => cch.data :transaction_ids
        ) AS TRANSACTION
    WHERE
        block_height BETWEEN 35858811
        AND 40171633
),
-- CTE to identify transactions that haven't been ingested yet
blocks AS (
    SELECT
        distinct(block_height)
    FROM
        collection_transactions ct
        LEFT JOIN {{ ref("streamline__complete_get_transactions_history") }}
        t
        ON ct.transaction_id = t.id
    WHERE
        t.id IS NULL
),
block_ids AS (
-- CTE to generate the block_ids for the missing block transactions
    SELECT
        data:id::STRING as block_id,
        block_number
    FROM
        {{ ref("streamline__complete_get_blocks_history")}}
    WHERE
        block_number BETWEEN 35858811
        AND 40171633
)
-- Generate the requests based on the missing block transactions
SELECT
    OBJECT_CONSTRUCT(
        'grpc',
        'proto3',
        'method',
        'get_transaction_by_block_i_d',
        'block_height',
        block_height :: INTEGER,
        'method_params',
        OBJECT_CONSTRUCT(
            'block_id',
            block_id
        )
    ) AS request
FROM
    blocks
JOIN
    block_ids on blocks.block_height = block_ids.block_number 
ORDER BY
    block_height ASC
