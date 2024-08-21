{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)",
    tags = ['nft', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH moment_events AS (

    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_id,
        event_index,
        event_type,
        event_contract,
        event_data,
        _inserted_timestamp,
        _partition_by_block_id
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        event_type IN (
            'MomentPurchased',
            'MomentLocked',
            'MomentCreated',
            'MomentNFTBurned',
            'MomentListed',
            'MomentDestroyed',
            'MomentWithdrawn',
            'MomentMinted',
            'MomentNFTMinted'
        )
{% if is_incremental() %}
AND
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['event_id']
    ) }} AS nft_moments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    moment_events
