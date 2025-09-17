-- depends_on: {{ ref('bronze__streamline_testnet_blocks') }}
-- depends_on: {{ ref('bronze__streamline_fr_testnet_blocks') }}

{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "_partition_by_block_id"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "block_timestamp::date",
    tags = ['testnet']
) }}

WITH

{% if is_incremental() %}
tx_count_lookback AS (
    -- lookback to ensure tx count is correct

    SELECT
        block_height
    FROM
        {{ this }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }} -- TODO, remove AFTER backfill is complete
        -- limit to 3 day lookback for performance
        AND _inserted_timestamp >= SYSDATE() - INTERVAL '3 days'
        AND (
            tx_count IS NULL
            OR collection_count != collection_count_agg
        )
),
{% endif %}

streamline_blocks AS (
    SELECT
        block_number,
        DATA: height :: STRING AS block_height,
        DATA: id :: STRING AS block_id,
        DATA :timestamp :: timestamp_ntz AS block_timestamp,
        ARRAY_SIZE(
            DATA :collection_guarantees :: ARRAY
        ) AS collection_count,
        DATA: parent_id :: STRING AS parent_id,
        DATA: signatures :: ARRAY AS signatures,
        DATA: collection_guarantees :: ARRAY AS collection_guarantees,
        DATA: block_seals :: ARRAY AS block_seals,
        _partition_by_block_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_testnet_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    OR block_height IN (
        SELECT
            block_height
        FROM
            tx_count_lookback
    )
{% else %}
    {{ ref('bronze__streamline_fr_testnet_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
collections AS (
    SELECT
        *
    FROM
        {{ ref('silver__testnet_collections') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR block_number IN (
        SELECT
            block_height
        FROM
            tx_count_lookback
    )
{% endif %}

),
tx_count AS (
    SELECT
        block_number AS block_height,
        SUM(tx_count) AS tx_count,
        COUNT(1) AS collection_count,
        MIN(_inserted_timestamp) AS _inserted_timestamp
    FROM
        collections
    GROUP BY
        1
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_height,
        NULL AS network_version,
        b.block_id AS id,
        b.block_timestamp,
        b.collection_count,
        IFF(
            b.collection_count = 0,
            b.collection_count,
            C.tx_count
        ) AS tx_count,
        b.parent_id,
        b.signatures,
        b.collection_guarantees,
        b.block_seals,
        C.collection_count AS collection_count_agg,
        b._partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number']
        ) }} AS blocks_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        b._inserted_timestamp
    FROM
        streamline_blocks b
        LEFT JOIN tx_count C USING (block_height)
)
SELECT
    *
FROM
    FINAL
