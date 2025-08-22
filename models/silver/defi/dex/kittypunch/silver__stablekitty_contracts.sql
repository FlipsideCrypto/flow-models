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
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        data,
        inserted_timestamp,
        modified_timestamp,
        CASE 
            WHEN LOWER(contract_address) = LOWER('0x4412140D52C1F5834469a061927811Abb6026dB7') THEN 'StableKittyFactoryNG'
            WHEN LOWER(contract_address) = LOWER('0xf0E48dC92f66E246244dd9F33b02f57b0E69fBa9') THEN 'TwoKittyFactory'  
            ELSE 'Unknown'
        END AS factory_type
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        LOWER(contract_address) = LOWER('0x4412140D52C1F5834469a061927811Abb6026dB7')
        OR
        LOWER(contract_address) = LOWER('0xf0E48dC92f66E246244dd9F33b02f57b0E69fBa9') 
             
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
        CASE 
            WHEN factory_type = 'StableKittyFactoryNG' 
            THEN CONCAT('0x', SUBSTR(data, 251, 40))
            
            WHEN factory_type = 'TwoKittyFactory' AND LENGTH(data) = 130 
            THEN CONCAT('0x', SUBSTR(data, 27, 40))
            ELSE NULL
        END AS token0_address,
        
        CASE 
            WHEN factory_type = 'StableKittyFactoryNG' AND TRY_TO_NUMBER(SUBSTR(data, 195, 64), 16) >= 2
            THEN CONCAT('0x', SUBSTR(data, 315, 40))
            
            WHEN factory_type = 'TwoKittyFactory' AND LENGTH(data) = 130 
            THEN CONCAT('0x', SUBSTR(data, 91, 40))
            ELSE NULL
        END AS token1_address,
        
        CASE 
            WHEN factory_type = 'StableKittyFactoryNG' AND TRY_TO_NUMBER(SUBSTR(data, 195, 64), 16) = 3
            THEN CONCAT('0x', SUBSTR(data, 379, 40))

            WHEN factory_type = 'StableKittyFactoryNG' AND TRY_TO_NUMBER(SUBSTR(data, 195, 64), 16) = 2
            THEN CONCAT('0x', SUBSTR(data, 315, 40))

            WHEN factory_type = 'TwoKittyFactory' AND LENGTH(data) = 66 
            THEN CONCAT('0x', SUBSTR(data, 27, 40))

            WHEN factory_type = 'TwoKittyFactory' AND LENGTH(data) = 130 
            THEN CONCAT('0x', SUBSTR(data, 91, 40))

            ELSE NULL
        END AS pool_address,
        factory_type
    FROM
        pool_deployed_events
    WHERE
        data IS NOT NULL
        OR topic_1 IS NOT NULL
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
        pool_address,
        factory_type
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