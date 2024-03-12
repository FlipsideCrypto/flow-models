{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'udf_bulk_grpc_us_east_2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "node_url":"access-001.mainnet18.nodes.onflow.org:9000",
            "external_table": "transaction_results_batch_mainnet_18",
            "sql_limit": "72000",
            "producer_batch_size": "8000",
            "worker_batch_size": "1000",
            "sql_source": "{{this.identifier}}",
            "concurrent_requests": "750"
        }
    )
) }}

WITH blocks AS (
-- CTE to identify blocks that doesn't have tx_results ingested for mainnet 18
    SELECT
        block_height
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_height BETWEEN 47169687
        AND 55114466
    EXCEPT
    SELECT
        distinct block_number AS block_height
    FROM
        {{ ref("streamline__complete_get_transaction_results_history") }}
    WHERE
        block_number BETWEEN 47169687
        AND 55114466
),
block_ids AS (
-- CTE to generate the block_ids for the missing block transactions
    SELECT
        data:id::STRING as block_id,
        block_number
    FROM
        {{ ref("streamline__complete_get_blocks_history")}}
    WHERE
        block_number BETWEEN 47169687
        AND 55114466
)
-- Generate the requests based on the missing block transactions
SELECT
    OBJECT_CONSTRUCT(
        'grpc',
        'proto3',
        'method',
        'get_transaction_results_by_block_i_d',
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
