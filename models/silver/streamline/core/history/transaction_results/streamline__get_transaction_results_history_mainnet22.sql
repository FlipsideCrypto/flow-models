{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc(object_construct('sql_source', '{{this.identifier}}','node_url','access-001.mainnet22.nodes.onflow.org:9000','external_table', 'transaction_results', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','30000')}}, 'worker_batch_size', {{var('worker_batch_size','3000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

-- CTE to get all transaction_ids from the complete_get_collections table
WITH collection_transactions AS (
    SELECT
        block_number AS block_height,
        transaction.value::STRING AS transaction_id
    FROM
        {{ ref("streamline__complete_get_collections") }} cc,
        LATERAL FLATTEN(input => cc.data:transaction_ids) AS transaction
),

-- CTE to identify transaction_results that haven't been ingested yet
transaction_results_to_ingest AS (
    SELECT
        ct.block_height,
        ct.transaction_id
    FROM
        collection_transactions ct
    LEFT JOIN
         {{ ref("streamline__complete_get_transaction_results") }} tr ON ct.transaction_id = tr.id
    WHERE
        tr.id IS NULL
)

-- Generate the requests column based on the missing transactions
SELECT
    OBJECT_CONSTRUCT(
        'grpc', 'proto3',
        'method', 'get_transaction_result',
        'block_height', block_height::INTEGER,
        'transaction_id', transaction_id::STRING,
        'method_params', OBJECT_CONSTRUCT('id',  transaction_id::STRING)
    ) AS request
FROM
    transaction_results_to_ingest
ORDER BY
    block_height ASC