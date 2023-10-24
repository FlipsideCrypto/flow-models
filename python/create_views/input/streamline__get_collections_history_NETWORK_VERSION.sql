{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc_us_east_2(object_construct('sql_source', '{{this.identifier}}','node_url','NETWORK_NODE_URL','external_table', 'transactions_NETWORK_VERSION', 'sql_limit', {{var('sql_limit','500000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
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
        {{ ref("streamline__complete_get_transactions") }}
),
tx AS (
    SELECT
        block_number AS block_height,
        DATA
    FROM
        {{ ref('streamline__complete_get_collections') }}
        JOIN blocks
        ON blocks.block_height = block_number
)
SELECT
    OBJECT_CONSTRUCT(
        'grpc', 'proto3',
        'method', 'get_transaction',
        'block_height', block_height::INTEGER,
        'transaction_id', transaction_id.value::string,
        'method_params', OBJECT_CONSTRUCT('id', transaction_id.value :: STRING)
    ) AS request
FROM
    tx,
    LATERAL FLATTEN(input => TRY_PARSE_JSON(DATA) :transaction_ids) AS transaction_id
WHERE
    block_height BETWEEN root_height
    AND end_height
ORDER BY
    block_height ASC