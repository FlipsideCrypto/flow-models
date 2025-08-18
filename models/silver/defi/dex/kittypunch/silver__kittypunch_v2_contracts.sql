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
        LOWER(contract_address) = LOWER('0x29372c22459a4e373851798bFd6808e71EA34A71')
        AND topic_0 = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' --PairCreated event signature
        
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
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS token_address_0,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS token_address_1,
        CONCAT('0x', SUBSTR(data, 27, 40)) AS pair_address,
        TRY_TO_NUMBER(SUBSTR(data, 67, 64), 16) AS pair_id,
        data AS raw_data,
        inserted_timestamp,
        modified_timestamp
    FROM
        pair_created_events
    WHERE
        data IS NOT NULL
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
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