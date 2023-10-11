{{ config(
    materialized = 'incremental',
    unique_key = 'block_height',
    tags = ['scheduled', 'streamline_scheduled', 'core']
) }}

WITH collections AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_collections') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
tx_count AS (
    SELECT
        block_number AS block_height,
        SUM(tx_count) AS tx_count,
        MIN(_inserted_timestamp) AS _inserted_timestamp
    FROM
        collections
    GROUP BY
        1
)
SELECT
    *
FROM
    tx_count
