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
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_evm_chainhead") }}
    )