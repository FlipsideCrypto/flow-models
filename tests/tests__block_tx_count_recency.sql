{{ config(
    severity = 'error',
    tags = ['observability']
) }}

WITH check_lag AS (

    SELECT
        {{ target.database }}.streamline.udf_get_chainhead() AS chainhead,
        (
            SELECT
                MAX(block_height)
            FROM
                {{ ref('silver_observability__block_tx_count') }}
        ) AS max_height,
        (
            chainhead - max_height < 25000
        ) AS is_recent
)
SELECT
    *
FROM
    check_lag
WHERE
    NOT is_recent
