{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('core__ez_bridge_transactions') }}
