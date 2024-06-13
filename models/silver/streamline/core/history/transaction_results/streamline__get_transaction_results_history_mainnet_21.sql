{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc_us_east_2(object_construct('sql_source', '{{this.identifier}}','node_url','access-001.mainnet21.nodes.onflow.org:9000','external_table', 'transaction_results_mainnet_21', 'sql_limit', '10', 'producer_batch_size', '5', 'worker_batch_size', '1', 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
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
        block_height BETWEEN 44950207
        AND 47169686
),
epoch_payments AS (
    SELECT
        block_number AS block_height,
        id AS transaction_id
    FROM
        {{ ref ('streamline__complete_get_transactions_history') }} t
    WHERE 
        (block_number BETWEEN 44950207 AND 47169686)
        AND
        data:script::string like '%import FlowEpoch%' and array_size(data:arguments::array) = 6
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
        LEFT JOIN epoch_payments ep
        ON ct.transaction_id = ep.transaction_id
    WHERE
        t.id IS NULL
        AND ep.transaction_id IS NULL
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
