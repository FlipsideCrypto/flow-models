{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_testnet_traces",
        "sql_limit" :"1000000",
        "async_concurrent_requests" :"10",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result"])}
    ),
    tags = ['streamline_history_evm_testnet']
) }}


WITH tbl AS (

    SELECT
        block_number
    FROM
        {{ ref('streamline__evm_testnet_blocks') }}
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref('streamline__complete_get_evm_testnet_traces') }}
    WHERE
        block_number IS NOT NULL
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        tbl
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
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'debug_traceBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(
                utils.udf_int_to_hex(block_number),
                OBJECT_CONSTRUCT(
                    'tracer', 'callTracer', 
                    'timeout', '180s'
                )
            )
        ),
        'Vault/prod/flow/quicknode/testnet'
    ) AS request
FROM
    ready_blocks
ORDER BY
    block_number ASC    