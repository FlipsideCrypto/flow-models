{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    cluster_by = ['block_timestamp::date', 'modified_timestamp::date'],
    unique_key = "ez_token_transfers_id",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,sender,recipient,token_contract);",
    tags = ['scheduled_non_core']
) }}

WITH pre_crescendo AS (

    SELECT
        block_height,
        block_timestamp,
        tx_id,
        sender,
        recipient,
        token_contract,
        amount,
        tx_succeeded,
        COALESCE (
            token_transfers_id,
            {{ dbt_utils.generate_surrogate_key(
                ['tx_id','sender', 'recipient','token_contract', 'amount']
            ) }}
        ) AS ez_token_transfers_id,
        inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ ref('silver__token_transfers_s') }}
    WHERE
        token_contract NOT IN (
            'A.c38aea683c0c4d38.ZelosAccountingToken',
            'A.f1b97c06745f37ad.SwapPair'
        )

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
post_crescendo AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        from_address AS sender,
        to_address AS recipient,
        token_contract,
        amount_adj :: FLOAT AS amount,
        tx_succeeded,
        token_transfers_id AS ez_token_transfers_id,
        inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ ref('silver__token_transfers') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *
FROM
    pre_crescendo
UNION ALL
SELECT
    *
FROM
    post_crescendo
