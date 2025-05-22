{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::date'],
    unique_key = 'bridge_complete_id',
    tags = ['bridge', 'scheduled']
) }}

WITH
layerzero AS (
    SELECT
        tx_hash AS tx_id,
        block_timestamp,
        block_number AS block_height,
        bridge_address,
        token_address,
        amount AS gross_amount,
        amount_fee,
        amount AS net_amount,
        CASE 
            WHEN direction = 'inbound' THEN destination_address
            ELSE NULL
        END AS destination_address,
        CASE 
            WHEN direction = 'outbound' THEN source_address
            ELSE NULL
        END AS source_address,
        CASE 
            WHEN direction = 'inbound' THEN destination_chain
            ELSE source_chain
        END AS destination_chain,
        CASE 
            WHEN direction = 'outbound' THEN destination_chain
            ELSE source_chain
        END AS source_chain,
        platform,
        inserted_timestamp,
        modified_timestamp,
        bridge_layerzero_id AS bridge_complete_id
    FROM
        {{ ref('silver_evm__bridge_layerzero_s') }}
    {% if is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)


SELECT
    tx_id,
    block_timestamp,
    block_height,
    bridge_address,
    token_address,
    gross_amount,
    amount_fee,
    net_amount,
    source_address,
    destination_address,
    source_chain,
    destination_chain,
    platform,
    bridge_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    layerzero