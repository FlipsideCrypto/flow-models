{{ config (
    materialized = 'table',
    tags = ['streamline_view']
) }}

SELECT
    live.udf_api(
        'GET',
        '{Service}/{Authentication}/v1/node_version_info',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'LiveQuery'
        ),
        OBJECT_CONSTRUCT(),
        'Vault/prod/flow/quicknode/mainnet'
    ) :data :compatible_range :end_height :: INT AS block_height
