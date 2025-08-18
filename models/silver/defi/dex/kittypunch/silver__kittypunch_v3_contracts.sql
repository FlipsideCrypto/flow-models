{{ config(
    materialized = 'table',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'kittypunch_v3_contracts_id',
    tags = ['scheduled_non_core', 'kittypunch', 'dex', 'v3', 'pools']
) }}

WITH pool_created_events AS (
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
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        LOWER(contract_address) = LOWER('0xf331959366032a634c7cAcF5852fE01ffdB84Af0')
        AND topic_0 = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' -- PoolCreated event signature
        AND block_timestamp >= '2025-04-01' -- V3 factory deployment
        
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '2025-04-01'::TIMESTAMP)
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
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS token_address_0,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS token_address_1,
        CASE 
            WHEN topic_3 = '0x0000000000000000000000000000000000000000000000000000000000000064' THEN 100
            WHEN topic_3 = '0x0000000000000000000000000000000000000000000000000000000000002710' THEN 10000
            WHEN topic_3 = '0x0000000000000000000000000000000000000000000000000000000000000bb8' THEN 3000
            ELSE TRY_TO_NUMBER(SUBSTR(topic_3, 3, 64), 16)
        END AS fee_tier,
        CASE 
            WHEN SUBSTR(data, 3, 64) = '0000000000000000000000000000000000000000000000000000000000000001' THEN 1
            WHEN SUBSTR(data, 3, 64) = '00000000000000000000000000000000000000000000000000000000000000c8' THEN 200
            WHEN SUBSTR(data, 3, 64) = '000000000000000000000000000000000000000000000000000000000000003c' THEN 60
            ELSE TRY_TO_NUMBER(SUBSTR(data, 3, 64), 16)
        END AS tick_spacing,
        CONCAT('0x', SUBSTR(data, 91, 40)) AS pool_address,
        data AS raw_data,
        inserted_timestamp,
        modified_timestamp
    FROM
        pool_created_events
    WHERE
        data IS NOT NULL
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
        AND topic_3 IS NOT NULL
),

FINAL AS (
    SELECT
        tx_hash AS tx_id,
        block_timestamp,
        block_number AS block_height,
        event_index,
        factory_address,
        'kittypunch' AS platform,
        'v3' AS platform_version,
        token_address_0 AS token0_address,
        token_address_1 AS token1_address,
        pool_address,
        COALESCE(fee_tier, 0) AS fee_tier,
        COALESCE(tick_spacing, 0) AS tick_spacing,
        CASE
            WHEN LOWER(token_address_0) <= LOWER(token_address_1)
            THEN CONCAT(token_address_0, '-', token_address_1, '-', COALESCE(fee_tier, 0))
            ELSE CONCAT(token_address_1, '-', token_address_0, '-', COALESCE(fee_tier, 0))
        END AS pool_name,
        CASE
            WHEN LOWER(token_address_0) = LOWER('0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e') 
                OR LOWER(token_address_1) = LOWER('0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e')
            THEN TRUE
            ELSE FALSE
        END AS contains_wflow,
        raw_data
    FROM
        parsed_pools
    WHERE
        token_address_0 != token_address_1
        AND pool_address != '0x0000000000000000000000000000000000000000'
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['token0_address', 'token1_address', 'pool_address', 'fee_tier']
    ) }} AS kittypunch_v3_contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL