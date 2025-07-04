{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    FALSE AS is_native,
    is_imputed,
    is_deprecated,
    is_verified,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_token_prices') }}
UNION ALL
SELECT
    HOUR,
    'A.1654653399040a61.FlowToken' AS token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    TRUE AS is_native,
    is_imputed,
    is_deprecated,
    TRUE AS is_verified,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_native_prices') }}
