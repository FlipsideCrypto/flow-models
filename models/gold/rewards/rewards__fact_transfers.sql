{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }} },
    tags = ['scheduled_non_core']
) }}

SELECT
    batch_id,
    created_at,
    batch_index,
    transfer_index,
    from_address,
    to_address,
    boxes,
    keys,
    points,
    points_transfers_id AS fact_transfers_id,
    request_date,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_api__points_transfers') }}
