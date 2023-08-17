{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_grpc(object_construct('sql_source', '{{this.identifier}}','node_url','access-001.mainnet22.nodes.onflow.org:9000','external_table', 'transactions', 'sql_limit', {{var('sql_limit','10000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','10000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
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
        block_number as block_height
    FROM
        {{ ref("streamline__complete_get_transactions") }}
),
tx AS (

    SELECT
        block_number as block_height,
        data
    FROM
        {{ ref('streamline__complete_get_collections') }}
    JOIN blocks ON blocks.block_height = block_number
)
SELECT
    OBJECT_CONSTRUCT(
        'grpc', 'proto3',
        'method', 'get_transaction',
        'block_height', block_height::INTEGER,
        'transaction_id', transaction_id.value::string,
        'method_params', OBJECT_CONSTRUCT('id',  transaction_id.value::string)
    ) AS request
FROM
    tx,
    LATERAL FLATTEN(input => TRY_PARSE_JSON(data):transaction_ids) AS transaction_id
WHERE
    block_height BETWEEN 47169687 AND 55114466 -- Mainnet22 block range
ORDER BY
    block_height ASC
