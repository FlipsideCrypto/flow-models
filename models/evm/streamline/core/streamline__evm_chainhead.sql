{{ config (
    materialized = 'table',
    tags = ['streamline_realtime_evm', 'streamline_history_evm']
) }}

SELECT
    live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'LiveQuery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        'Vault/prod/flow/quicknode/mainnet'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number