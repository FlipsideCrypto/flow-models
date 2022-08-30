{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('core__ez_dex_swaps') }}
