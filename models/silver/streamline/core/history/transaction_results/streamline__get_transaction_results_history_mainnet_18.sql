{{ config (
    materialized = "view",

    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'udf_bulk_grpc_us_east_2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "node_url":"access-001.mainnet18.nodes.onflow.org:9000",
            "external_table": "transaction_results_mainnet_18",
            "sql_limit": "25000",
            "producer_batch_size": "1000",
            "worker_batch_size": "200",
            "sql_source": "{{this.identifier}}"
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
        block_height BETWEEN 31735955
        AND 35858810
),
-- CTE to identify transactions that haven't been ingested yet
transactions_to_ingest AS (
    SELECT
        ct.block_height,
        ct.transaction_id
    FROM
        collection_transactions ct
        LEFT JOIN {{ ref("streamline__complete_get_transaction_results_history") }}
        t
        ON ct.transaction_id = t.id
    WHERE
        t.id IS NULL
) -- Generate the requests based on the missing transactions
SELECT
    OBJECT_CONSTRUCT(
        'grpc',
        'proto3',
        'method',
        'get_transaction_result',
        'block_height',
        block_height :: INTEGER,
        'transaction_id',
        transaction_id,
        'method_params',
        OBJECT_CONSTRUCT(
            'id',
            transaction_id
        )
    ) AS request
FROM
    transactions_to_ingest
ORDER BY
    block_height ASC
