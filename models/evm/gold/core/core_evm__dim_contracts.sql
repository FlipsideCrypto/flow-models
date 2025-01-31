{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, symbol, name), SUBSTRING(address, symbol, name)",
    tags = ['evm']
) }}

SELECT
    LOWER(COALESCE(c0.created_contract_address,c1.contract_address)) AS address,
    c1.token_symbol AS symbol,
    c1.token_name AS NAME,
    c1.token_decimals AS decimals,
    c0.block_number AS created_block_number,
    c0.block_timestamp AS created_block_timestamp,
    c0.tx_hash AS created_tx_hash,
    c0.creator_address AS creator_address,
    c0.created_contracts_id AS dim_contracts_id,
    GREATEST(COALESCE(c0.inserted_timestamp, '2000-01-01'), COALESCE(c1.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(c0.modified_timestamp, '2000-01-01'), COALESCE(c1.modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    {{ ref('silver_evm__created_contracts') }}
    c0
    FULL OUTER JOIN {{ ref('silver_evm__contracts') }}
    c1
    ON LOWER(
        c0.created_contract_address
    ) = LOWER(
        c1.contract_address
    )
{% if is_incremental() %}
WHERE
    c0.modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
    OR
    c1.modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}