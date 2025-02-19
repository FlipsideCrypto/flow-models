{{ config(
    materialized = "view",
    tags = ['streamline_realtime_evm', 'streamline_history_evm']
) }}

SELECT
    _id AS block_number
FROM
    {{ source(
        'silver_crosschain',
        'number_sequence'
    ) }}
WHERE
    _id <= (SELECT block_number FROM {{ ref('streamline__evm_chainhead') }})