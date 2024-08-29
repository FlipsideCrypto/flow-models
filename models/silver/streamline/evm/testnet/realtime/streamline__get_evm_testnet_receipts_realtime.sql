{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_testnet_receipts_stg",
        "sql_limit" :"250000",
        "producer_batch_size" :"50000",
        "worker_batch_size" :"10000",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- TODO refactor this to only request for blocks w a confirmed tx

WITH tbl AS (

    SELECT
        block_height
    FROM
        {{ ref('streamline__evm_testnet_blocks') }}
    EXCEPT
    SELECT
        block_number AS block_height
    FROM
        {{ ref('streamline__complete_get_evm_testnet_receipts') }}
)
SELECT
    block_height,
    DATE_PART(epoch_second, SYSDATE())::STRING AS request_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    ROUND(
        block_height,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_height,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockReceipts',
            'params',
            ARRAY_CONSTRUCT(
                CONCAT('0x', TRIM(to_char(block_height, 'XXXXXXXX')))
            )
        ),
        'Vault/{{ target.name }}/flow/testnet'
    ) AS request
FROM
    tbl
ORDER BY
    block_height ASC
