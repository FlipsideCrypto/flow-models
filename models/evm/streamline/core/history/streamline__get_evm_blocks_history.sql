{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_blocks",
        "sql_limit" :"100000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_history_evm']
) }}

WITH tbl AS (
    SELECT
        block_number
    FROM
        {{ ref('streamline__evm_blocks') }}
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref('streamline__complete_get_evm_blocks') }}
)
SELECT
    block_number,
    DATE_PART(epoch_second, SYSDATE()) :: STRING AS request_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    ROUND(
        block_number,
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
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(
                utils.udf_int_to_hex(block_number),
                TRUE -- Include transactions
            )
        ),
        'Vault/{{ target.name }}/flow/evm/mainnet'
    ) AS request
FROM
    tbl
ORDER BY
    block_number DESC
