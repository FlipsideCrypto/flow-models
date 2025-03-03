{% set full_reload_mode = var('SCORES_FULL_RELOAD_MODE', false) %}

{{ config (
    materialized = "view",
    tags = ['scores']
) }}

{% if execute %}
    {{ log("==========================================", info=True) }}
    {{ log("Generating date spine for blockchain: " ~ blockchain, info=True) }}
    {{ log("Backfill mode: " ~ full_reload_mode, info=True) }}
    {{ log("==========================================", info=True) }}
{% endif %}

WITH chain_dates AS (
    SELECT
        block_timestamp :: DATE AS block_date,
        count(distinct date_trunc('hour', block_timestamp)) AS n_hours
    FROM {{ ref('core_evm__fact_blocks') }}

{% if not full_reload_mode %}
    WHERE block_timestamp :: DATE > DATEADD('day', -120, SYSDATE() :: DATE)
{% endif %}
    GROUP BY ALL
),
date_spine AS (

{% if full_reload_mode %}
    SELECT
        date_day
    FROM
        {{ ref('scores__dates') }}
    WHERE
        day_of_week = 1
        AND date_day < DATEADD('day', -90, SYSDATE() :: DATE) -- every sunday, excluding last 90 days
        AND date_day <= '2024-07-01'
    UNION
{% endif %}

    SELECT
        date_day
    FROM
        {{ ref('scores__dates') }}
    WHERE
        date_day >= '2024-07-01'
        and date_day <= (SELECT MAX(block_date) FROM chain_dates where n_hours = 24)
),
day_of_chain AS (
    SELECT
        block_date,
        ROW_NUMBER() over (ORDER BY block_date ASC) AS chain_day
    FROM
        chain_dates
),
exclude_first_90_days AS (
    SELECT
        block_date
    FROM
        day_of_chain

{% if full_reload_mode %}
    WHERE chain_day >= 90
{% endif %}

),
eligible_dates AS (
    SELECT
        block_date
    FROM
        exclude_first_90_days
    JOIN date_spine ON date_day = block_date
)
SELECT
    block_date
FROM
    eligible_dates
ORDER BY block_date ASC