{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

WITH ch AS (

    SELECT
        block_height
    FROM
        {{ ref('streamline__chainhead') }}
)
SELECT
    _id AS block_height
FROM
    {{ source(
        'silver_crosschain',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            block_height
        FROM
            ch
    )
