{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc(object_construct('sql_source', '{{this.identifier}}','node_url','access.mainnet.nodes.onflow.org:9000','external_table', 'transaction_results', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_realtime']
) }}

WITH last_3_days AS (
{% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        MAX(root_height) AS block_height
    FROM
        {{ ref('seeds__network_version') }}
    {% elif var('STREAMLINE_BACKFILL', False) %}
    SELECT
        107600000 AS block_height
    
    {% else %}
    SELECT
        MAX(block_height) - 210000 AS block_height
    FROM
        {{ ref('streamline__blocks') }}
    {% endif %}),
    collection_transactions AS (
        SELECT
            block_number AS block_height,
            TRANSACTION.value :: STRING AS transaction_id
        FROM
            {{ ref("streamline__complete_get_collections") }}
            cc,
            LATERAL FLATTEN(
                input => cc.data :transaction_ids
            ) AS TRANSACTION
        WHERE
            block_height >= (
                SELECT
                    block_height
                FROM
                    last_3_days
            )
    ),
    -- CTE to identify transactions that haven't been ingested yet
    transactions_to_ingest AS (
        SELECT
            ct.block_height,
            ct.transaction_id
        FROM
            collection_transactions ct
            LEFT JOIN {{ ref("streamline__complete_get_transaction_results") }}
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
        transaction_id :: STRING,
        'method_params',
        OBJECT_CONSTRUCT(
            'id',
            transaction_id :: STRING
        )
    ) AS request
FROM
    transactions_to_ingest

ORDER BY
    block_height DESC
