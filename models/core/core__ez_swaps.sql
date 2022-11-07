{{ config (
    materialized = 'view',
    tags = ['ez']
) }}

SELECT
    *
FROM
    {{ ref('silver__swaps') }}
