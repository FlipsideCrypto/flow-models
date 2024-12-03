-- depends_on: {{ ref('bronze_api__minting_assets') }}
-- depends_on: {{ ref('bronze_api__FR_minting_assets') }}
{{ config(
    materialized = 'incremental',
    unique_key = "minting_assets_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE'],
    tags = ['streamline_non_core']
) }}

WITH bronze AS (

    SELECT
        partition_key,
        DATA,
        VALUE :API_LIMIT :: INTEGER AS api_limit,
        ARRAY_SIZE(
            DATA :data :: ARRAY
        ) AS item_count,
        DATA :data [0] :id :: STRING AS first_item_id,
        DATA :data [item_count - 1] :id :: STRING AS last_item_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_api__minting_assets') }}
WHERE
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
{% else %}
    {{ ref('bronze_api__FR_minting_assets') }}
{% endif %}
)
SELECT
    partition_key,
    item_count,
    api_limit,
    first_item_id AS request_first_item_id,
    last_item_id AS request_last_item_id,
    VALUE :createdAt :: timestamp_ntz AS created_at,
    VALUE :id :: STRING AS item_id,
    INDEX :: INTEGER AS INDEX,
    VALUE :: variant AS DATA,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['item_id', 'partition_key']
    ) }} AS minting_assets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze,
    LATERAL FLATTEN(
        input => DATA :data :: ARRAY
    ) 

