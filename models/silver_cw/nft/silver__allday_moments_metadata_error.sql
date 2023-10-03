{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

SELECT
    *
FROM
    {{ ref('silver__nft_allday_metadata') }}
WHERE
    nft_id IS NULL
    OR nft_collection IS NULL
    OR nflallday_id IS NULL
    OR serial_number IS NULL
    OR moment_tier IS NULL
    OR total_circulation IS NULL
    OR moment_description IS NULL
    OR player IS NULL
    OR team IS NULL
    OR season IS NULL
    OR week IS NULL
    OR classification IS NULL
    OR play_type IS NULL
    OR moment_date IS NULL
    OR series IS NULL
    OR set_name IS NULL
    OR video_urls IS NULL
    OR moment_stats_full IS NULL
