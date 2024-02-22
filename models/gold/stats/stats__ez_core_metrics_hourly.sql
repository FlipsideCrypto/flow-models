{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

SELECT
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_from_count,
    unique_payer_count,
    total_fees AS total_fees_native,
    ROUND(
        total_fees * p.close,
        2
    ) AS total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    s.inserted_timestamp AS inserted_timestamp,
    s.modified_timestamp AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
    s
    LEFT JOIN {{ ref('price__fact_hourly_prices') }}
    p
    ON s.block_timestamp_hour = p.RECORDED_HOUR
    AND p.token = 'Flow'