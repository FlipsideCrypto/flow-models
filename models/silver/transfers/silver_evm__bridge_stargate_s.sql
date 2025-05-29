{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['modified_timestamp::date'],
    unique_key = 'tx_id',
    tags = ['bridge', 'scheduled', 'streamline_scheduled', 'scheduled_non_core', 'stargate']
) }}
{# {% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(block_timestamp) :: DATE AS block_timestamp
FROM
    {{ ref('core_evm__ez_decoded_event_logs') }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '2 hours'
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_date = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}

#}
WITH pools AS (
    SELECT
        pool_address,
        LOWER(token_address) AS token_address
    FROM
        {{ ref('silver_evm__bridge_stargate_create_pool') }}
),
events AS (
    SELECT
        block_number AS block_height,
        block_timestamp,
        tx_hash AS tx_id,
        event_index,
        contract_address AS event_contract,
        p.token_address,
        event_name AS event_type,
        decoded_log AS event_data,
        modified_timestamp,
        inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }}
        e
        INNER JOIN pools p
        ON e.contract_address = p.pool_address
    WHERE
        block_timestamp :: DATE >= '2025-01-29' -- first date of Stargate events
        AND event_name IN (
            'OFTSent',
            'OFTReceived'
        )
        AND event_data IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
-- Process OFTSent events (outbound transfers)
oft_sent_events AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        event_contract AS bridge_contract,
        event_data :amountSentLD :: DOUBLE AS sent_amount,
        event_data :amountReceivedLD :: DOUBLE AS received_amount,
        COALESCE(
            sent_amount - received_amount,
            0
        ) AS fee_amount,
        LOWER(
            event_data :fromAddress :: STRING
        ) AS flow_wallet_address,
        token_address,
        event_data :dstEid :: NUMBER AS dst_endpoint_id,
        30362 AS src_endpoint_id,
        event_data :guid :: STRING AS transfer_guid,
        'outbound' AS direction,
        'stargate' AS bridge,
        _inserted_timestamp
    FROM
        events
    WHERE
        event_type = 'OFTSent'
),
-- Process OFTReceived events (inbound transfers)
oft_received_events AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        event_contract AS bridge_contract,
        event_data :amountReceivedLD :: DOUBLE AS received_amount,
        0 AS fee_amount,
        received_amount AS net_amount,
        LOWER(
            event_data :toAddress :: STRING
        ) AS flow_wallet_address,
        token_address,
        NULL AS dst_endpoint_id,
        event_data :srcEid :: NUMBER AS src_endpoint_id,
        event_data :guid :: STRING AS transfer_guid,
        'inbound' AS direction,
        'stargate' AS bridge,
        _inserted_timestamp
    FROM
        events
    WHERE
        event_type = 'OFTReceived'
),
combined_events AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        bridge_contract,
        sent_amount AS gross_amount,
        fee_amount,
        received_amount AS net_amount,
        flow_wallet_address,
        token_address,
        src_endpoint_id,
        dst_endpoint_id,
        transfer_guid,
        direction,
        bridge,
        _inserted_timestamp
    FROM
        oft_sent_events
    UNION ALL
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        bridge_contract,
        received_amount AS gross_amount,
        fee_amount,
        net_amount,
        flow_wallet_address,
        token_address,
        src_endpoint_id,
        dst_endpoint_id,
        transfer_guid,
        direction,
        bridge,
        _inserted_timestamp
    FROM
        oft_received_events
),
{#
-- Join with token transfer data to get token information
token_transfers AS (
    SELECT
        tx_hash AS tx_id,
        contract_address AS token_address,
        raw_amount AS amount,
        NAME AS token_name,
        symbol AS token_symbol,
        decimals,
        event_index AS token_event_index,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS rn
    FROM
        {{ ref('core_evm__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_id
            FROM
                combined_events
        )

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{min_block_date}}'
{% endif %}
),
#}
endpoint_ids AS (
    SELECT
        endpoint_id,
        LOWER(blockchain) AS blockchain
    FROM
        {{ ref('seeds__layerzero_endpoint_ids') }}
)
SELECT
    ce.tx_id,
    ce.block_timestamp,
    ce.block_height,
    ce.bridge_contract AS bridge_address,
    ce.token_address,
    ce.gross_amount,
    ce.fee_amount AS amount_fee,
    ce.net_amount,
    ce.flow_wallet_address,
    CASE
        WHEN ce.direction = 'outbound' THEN 'flow_evm'
        ELSE COALESCE(
            src.blockchain,
            'other_chains'
        )
    END AS source_chain,
    CASE
        WHEN ce.direction = 'inbound' THEN 'flow_evm'
        ELSE COALESCE(
            dst.blockchain,
            'other_chains'
        )
    END AS destination_chain,
    ce.direction,
    ce.bridge AS platform,
    ce.transfer_guid,
    ce._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['ce.tx_id', 'ce.event_index']) }} AS bridge_startgate_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combined_events ce {# LEFT JOIN token_transfers tt
    ON ce.tx_id = tt.tx_id
    AND ce.gross_amount = tt.amount #}
    LEFT JOIN endpoint_ids src
    ON src.endpoint_id = ce.src_endpoint_id
    LEFT JOIN endpoint_ids dst
    ON dst.endpoint_id = ce.dst_endpoint_id
