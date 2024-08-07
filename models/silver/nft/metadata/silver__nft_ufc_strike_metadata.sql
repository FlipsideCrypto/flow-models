{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['nft_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(nft_id);",
    tags = ['scheduled_non_core']
) }}

WITH metadata AS (

    SELECT
        token_id,
        edition,
        owner,
        listing_id,
        set_name,
        metadata,
        _inserted_timestamp,
        _filename
    FROM
        {{ source(
            'flow_bronze',
            'ufc_strike_metadata'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    token_id AS nft_id,
    edition AS serial_number,
    owner,
    listing_id,
    metadata :SET :: STRING AS set_name,
    set_name AS set_description,
    metadata,
    _inserted_timestamp,
    _filename,
    {{ dbt_utils.generate_surrogate_key(
            ['nft_id']
    ) }} AS nft_ufc_strike_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{invocation_id}}' AS _invocation_id
FROM
    metadata
