{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('streamline__all_topshot_moments_minted_metadata_needed') }}
