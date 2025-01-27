{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_traces_v2",
        "sql_limit" :"1000000",
        "producer_batch_size" :"2000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result"])}
    ),
    tags = ['streamline_history_evm']
) }}


WITH tbl AS (

    SELECT
        block_number
    FROM
        {{ ref('streamline__evm_blocks') }}
    WHERE block_number IS NOT NULL
        and block_number in (select distinct block_number from {{ ref('core_evm__fact_transactions') }})
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref('streamline__complete_get_evm_traces') }}
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
        'Vault/{{ target.name }}/flow/evm/mainnet'
    ) AS request
FROM
    ready_blocks
ORDER BY
    block_number DESC
limit 1000000