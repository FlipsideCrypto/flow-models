{{ config(
    materialized = 'incremental',
    unique_key = 'ez_transaction_actors_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = 'block_timestamp::date',
    post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,actors);',
    tags = ['scheduled_core']
) }}

WITH silver_actors AS (

    SELECT
        tx_id,
        block_height,
        block_timestamp,
        payer,
        proposer,
        authorizers,
        address
    FROM
        {{ ref('silver__transaction_actors') }}

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
generate_actors_array AS (
    SELECT
        tx_id,
        ARRAY_AGG(
            DISTINCT address
        ) AS actors_array
    FROM
        silver_actors
    GROUP BY
        tx_id
),
FINAL AS (
    SELECT
        B.block_height,
        B.block_timestamp,
        A.tx_id,
        array_distinct(
            ARRAY_CAT(
                ARRAY_CAT(ARRAY_CONSTRUCT(payer, proposer), authorizers),
                actors_array
            )
        ) AS actors
    FROM
        generate_actors_array A
        LEFT JOIN silver_actors b
        ON A.tx_id = b.tx_id
)
SELECT
    block_height,
    block_timestamp,
    tx_id,
    actors,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS ez_transaction_actors_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL
