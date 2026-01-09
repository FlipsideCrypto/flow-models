{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'kittypunch_v2_contracts_id',
    tags = ['scheduled_non_core', 'kittypunch', 'dex', 'v2', 'pairs']
) }}

WITH pair_created_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        decoded_log,
        full_decoded_log,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }}
    WHERE
        LOWER(contract_address) = LOWER('0x29372c22459a4e373851798bFd6808e71EA34A71')
        AND (
            topic_0 = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' --PairCreated event signature
            OR event_name = 'PairCreated'
        )
        
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '2000-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}
),

parsed_pairs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address AS factory_address,
        -- Handle multiple possible field name patterns
        COALESCE(
            decoded_log:token0::STRING,
            decoded_log:tokenA::STRING
        ) AS token_address_0,
        COALESCE(
            decoded_log:token1::STRING,
            decoded_log:tokenB::STRING
        ) AS token_address_1,
        COALESCE(
            decoded_log:pair::STRING,
            decoded_log:pool::STRING,
            decoded_log:address::STRING
        ) AS pair_address,
        COALESCE(
            decoded_log:pairId::NUMBER,
            decoded_log:id::NUMBER,
            0
        ) AS pair_id,
        full_decoded_log AS raw_data,
        inserted_timestamp,
        modified_timestamp
    FROM
        pair_created_events
    WHERE
        decoded_log IS NOT NULL
        AND (
            decoded_log:token0 IS NOT NULL OR decoded_log:tokenA IS NOT NULL
        )
        AND (
            decoded_log:token1 IS NOT NULL OR decoded_log:tokenB IS NOT NULL
        )
),
FINAL AS (
    SELECT
        tx_hash AS tx_id,
        block_timestamp,
        block_number AS block_height,
        event_index,
        factory_address,
        'kittypunch' AS platform,
        'v2' AS platform_version,
        token_address_0 AS token0_address,
        token_address_1 AS token1_address,
        pair_address,
        COALESCE(pair_id, 0) AS pair_id,
        CASE
            WHEN LOWER(token_address_0) <= LOWER(token_address_1)
            THEN CONCAT(token_address_0, '-', token_address_1)
            ELSE CONCAT(token_address_1, '-', token_address_0)
        END AS pair_name,
        CASE
            WHEN LOWER(token_address_0) = LOWER('0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e') 
                OR LOWER(token_address_1) = LOWER('0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e')
            THEN TRUE
            ELSE FALSE
        END AS contains_wflow,
        raw_data
    FROM
        parsed_pairs
    WHERE
        token_address_0 != token_address_1
        AND pair_address != '0x0000000000000000000000000000000000000000'
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['token0_address', 'token1_address', 'pair_address']
    ) }} AS kittypunch_v2_contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL