{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }} },
    tags = ['scheduled_non_core']
) }}

SELECT
    address,
    boxes,
    boxes_opened,
    keys,
    points,
    request_date,
    reward_points_id AS fact_balances_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_api__reward_points') }}
