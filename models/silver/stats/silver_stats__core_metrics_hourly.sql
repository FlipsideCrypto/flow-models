{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated', 'scheduled_non_core']
) }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '2 hours'
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}

WITH fees AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        SUM(
            event_data :amount :: FLOAT
        ) AS total_fees
    FROM
        {{ ref('core__fact_events') }}
        -- TODO: change this to silver when the backfill is done
    WHERE
        event_type = 'FeesDeducted'
        AND block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
),
transactions AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        COUNT(
            DISTINCT tx_id
        ) AS transaction_count,
        COUNT(
            DISTINCT CASE
                WHEN tx_succeeded THEN tx_id
            END
        ) AS transaction_count_success,
        COUNT(
            DISTINCT CASE
                WHEN NOT tx_succeeded THEN tx_id
            END
        ) AS transaction_count_failed,
        COUNT(
            DISTINCT proposer
        ) AS unique_from_count,
        COUNT(
            payer
        ) AS unique_payer_count,
        MAX(inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('core__fact_transactions') }} AS tx
    WHERE
        block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
)
SELECT
    tx.*,
    COALESCE(
        total_fees,
        0
    ) AS total_fees,
    -- As we are missing data, we miss the fee events. We need to coalesce to 0
    {{ dbt_utils.generate_surrogate_key(
        ['tx.block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transactions AS tx
    LEFT JOIN fees
    ON tx.block_timestamp_hour = fees.block_timestamp_hour
