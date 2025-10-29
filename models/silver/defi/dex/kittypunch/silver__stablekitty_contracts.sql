{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'stablekitty_contracts_id',
    tags = ['scheduled_non_core', 'stablekitty', 'dex', 'pools']
) }}

WITH pool_deployed_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        decoded_log,
        full_decoded_log,
        event_name,
        inserted_timestamp,
        modified_timestamp,
        CASE 
            WHEN LOWER(contract_address) = LOWER('0x4412140D52C1F5834469a061927811Abb6026dB7') THEN 'StableKittyFactoryNG'
            WHEN LOWER(contract_address) = LOWER('0xf0E48dC92f66E246244dd9F33b02f57b0E69fBa9') THEN 'TwoKittyFactory'  
            ELSE 'Unknown'
        END AS factory_type
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }}
    WHERE
        (
            LOWER(contract_address) = LOWER('0x4412140D52C1F5834469a061927811Abb6026dB7')
            OR LOWER(contract_address) = LOWER('0xf0E48dC92f66E246244dd9F33b02f57b0E69fBa9')
        )
        AND (
            event_name IN ('PlainPoolDeployed', 'PoolDeployed', 'Deployed')
            OR topic_0 = '0xd1d60d4611e4091bb2e5f699eeb79136c21ac2305ad609f3de569afc3471eecc' -- PlainPoolDeployed
        )
             
            -- Note: TriKittyFactory (0xebd098c60b1089f362AC9cfAd9134CBD29408226) has no deployment events
            -- This factory may not be active or may use a different deployment mechanism
        
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '2000-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}
),

parsed_pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address AS factory_address,
        -- Use decoded log fields for token addresses
        CASE 
            WHEN decoded_log:coins IS NOT NULL AND ARRAY_SIZE(decoded_log:coins) >= 1
            THEN decoded_log:coins[0]::STRING
            WHEN decoded_log:token0 IS NOT NULL
            THEN decoded_log:token0::STRING
            ELSE NULL
        END AS token0_address,
        
        CASE 
            WHEN decoded_log:coins IS NOT NULL AND ARRAY_SIZE(decoded_log:coins) >= 2
            THEN decoded_log:coins[1]::STRING
            WHEN decoded_log:token1 IS NOT NULL
            THEN decoded_log:token1::STRING
            ELSE NULL
        END AS token1_address,
        
        CASE 
            WHEN decoded_log:coins IS NOT NULL AND ARRAY_SIZE(decoded_log:coins) >= 3
            THEN decoded_log:coins[2]::STRING
            ELSE NULL
        END AS token2_address,
        
        -- Pool address from decoded log
        COALESCE(
            decoded_log:pool::STRING,
            decoded_log:poolAddress::STRING,
            decoded_log:address::STRING
        ) AS pool_address,
        
        factory_type,
        full_decoded_log AS raw_data
    FROM
        pool_deployed_events
    WHERE
        decoded_log IS NOT NULL
),

FINAL AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number AS block_height,
        event_index,
        factory_address,
        'stablekitty' AS platform,
        'stable' AS platform_version,
        token0_address,
        token1_address,
        token2_address,
        pool_address,
        factory_type,
        raw_data
    FROM
        parsed_pools
    WHERE
        pool_address IS NOT NULL
        AND pool_address != '0x0000000000000000000000000000000000000000'
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address', 'factory_address']
    ) }} AS stablekitty_contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL