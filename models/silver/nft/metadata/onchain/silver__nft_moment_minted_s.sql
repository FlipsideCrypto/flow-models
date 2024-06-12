{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', event_contract, edition_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH events AS (

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
        _partition_by_block_id,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        event_type = 'MomentNFTMinted'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
org AS (
    SELECT
        tx_id,
        event_id,
        block_timestamp,
        event_contract,
        event_data :editionID :: STRING AS edition_id,
        event_data :id :: STRING AS nft_id,
        event_data :serialNumber :: STRING AS serial_number,
        _inserted_timestamp,
        _partition_by_block_id
    FROM
        events
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['event_id']
    ) }} AS nft_moment_minted_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    org
