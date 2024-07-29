{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_testnet_blocks",
        "sql_limit" :"100000",
        "producer_batch_size" :"2000",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}" }
    )
) }}

WITH tbl AS (

    SELECT
        block_height
    FROM
        {{ ref('streamline__evm_testnet_blocks') }}
    EXCEPT
    SELECT
        block_number AS block_height
    FROM
        {{ ref('streamline__complete_get_evm_testnet_blocks') }}
)
SELECT
    block_height,
    ROUND(
        block_height,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        'https://previewnet.evm.nodes.onflow.org',
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
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(
                CONCAT('0x', TRIM(to_char(block_height, 'XXXXXXXX'))),
                TRUE
            )
        )
    ) AS request
FROM
    tbl
ORDER BY
    block_height DESC
