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
    ),
    tags = ['streamline_realtime_evm']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_evm_block_lookback") }}
),
tbl AS (

    SELECT
        block_number
    FROM
        {{ ref('streamline__evm_blocks') }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref('streamline__complete_get_evm_receipts') }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        tbl
    UNION ALL
    SELECT
        block_number
    FROM
        {{ ref("_missing_receipts") }}
)
SELECT
    block_number,
    DATE_PART(epoch_second, SYSDATE())::STRING AS request_timestamp,
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
            'eth_getBlockReceipts',
            'params',
            ARRAY_CONSTRUCT(
                CONCAT('0x', TRIM(to_char(block_number, 'XXXXXXXX')))
            )
        ),
        'Vault/{{ target.name }}/flow/evm'
    ) AS request
FROM
    ready_blocks
ORDER BY
    block_number ASC
