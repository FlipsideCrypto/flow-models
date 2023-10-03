{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)",
    tags = ['nft', 'scheduled', 'chainwalkers_scheduled']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
moment_events AS (
    SELECT
        *
    FROM
        events
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
)
SELECT
    *
FROM
    moment_events
