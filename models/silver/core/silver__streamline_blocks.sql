-- depends_on: {{ ref('bronze__streamline_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date",
    tags = ['streamline_load', 'core']
) }}

WITH streamline_blocks AS (

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
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
network_version AS (
    SELECT
        root_height,
        network_version,
        COALESCE(LAG(root_height) over (
    ORDER BY
        network_version DESC) - 1, 'inf' :: FLOAT) AS end_height
    FROM
        {{ ref('seeds__network_version') }}
),
FINAL AS (
    SELECT
        b.block_number,
        b.block_height,
        v.network_version,
        b.block_id AS id,
        b.block_timestamp,
        b.collection_count,
        C.tx_count,
        b.parent_id,
        b.signatures,
        b.collection_guarantees,
        b.block_seals,
        b._partition_by_block_id,
        b._inserted_timestamp
    FROM
        streamline_blocks b
        LEFT JOIN network_version v
        ON b.block_height BETWEEN v.root_height
        AND v.end_height
        LEFT JOIN {{ ref('silver__block_tx_count') }} C USING (block_height)
)
SELECT
    *
FROM
    FINAL
