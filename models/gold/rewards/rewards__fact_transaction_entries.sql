{{ config(
    materialized = 'view',
    tags = ['streamline_non_core']
)}}

SELECT
    entry_id,
    created_at,
    direction,
    amount,
    amount_start,
    amount_end,
    idempotency_key,
    organization_id,
    website_id,
    account_id,
    user_id,
    user_wallet_address,
    transaction_id,
    transaction_description,
    transaction_type,
    rule_id,
    rule_type,
    rule_name,
    rule_description,
    rule_metadata,
    DATA,
    partition_key,
    INDEX,
    _inserted_timestamp,
    reward_points_spend_id AS fact_transaction_entries_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver_api__reward_points_spend') }}
