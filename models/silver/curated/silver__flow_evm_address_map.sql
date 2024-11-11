{{ config(
    materialized = 'incremental',
    unique_key = 'flow_evm_address_map_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::date', 'modified_timestamp::date'],
    tags = ['scheduled_non_core']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        block_timestamp :: DATE >= '2024-09-02'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
coa_creation AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        CONCAT(
            '0x',
            event_data :address :: STRING
        ) AS evm_address
    FROM
        events
    WHERE
        event_contract = 'A.e467b9dd11fa00df.EVM'
        AND event_type = 'CadenceOwnedAccountCreated'
),
txs AS (
    SELECT
        tx_id,
        block_height,
        authorizers
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        block_timestamp :: DATE >= '2024-09-02'
        AND tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                events
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
get_flow_address AS (
    SELECT
        tx_id,
        block_height,
        event_index,
        event_type,
        event_data :address :: STRING AS flow_address
    FROM
        events
    WHERE
        event_contract = 'flow'
        AND event_type = 'CapabilityPublished'
        AND tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                coa_creation
        ) -- a transaction may emit multiple CapabilityPublished events
        qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        event_index) = 1)
),
map_addresses AS (
    SELECT
        A.tx_id,
        A.block_timestamp,
        A.block_height,
        A.evm_address,
        COALESCE(
            b.flow_address,
            C.authorizers [0] :: STRING
        ) AS flow_address,
        b.flow_address IS NULL AS used_authorizer
    FROM
        coa_creation A
        LEFT JOIN get_flow_address b
        ON A.tx_id = b.tx_id
        AND A.block_height = b.block_height
        LEFT JOIN txs C
        ON A.tx_id = C.tx_id
        AND A.block_height = C.block_height
)
SELECT
    tx_id,
    block_timestamp,
    block_height,
    evm_address,
    flow_address,
    used_authorizer,
    {{ dbt_utils.generate_surrogate_key(['evm_address', 'flow_address']) }} AS flow_evm_address_map_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    map_addresses
