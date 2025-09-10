{{ config(
    materialized = "view",
    tags = ['streamline_realtime_evm_testnet']
) }}

SELECT
    _id AS block_number
FROM
    {{ source(
        'silver_crosschain',
        'number_sequence'
    ) }}
WHERE
    _id <= (SELECT block_number FROM {{ ref('streamline__evm_testnet_chainhead') }})