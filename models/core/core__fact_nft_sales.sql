{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('core__ez_nft_sales') }}
