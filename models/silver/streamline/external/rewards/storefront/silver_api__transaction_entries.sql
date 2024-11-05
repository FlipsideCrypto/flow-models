-- depends_on: {{ ref('bronze_api__transaction_entries') }}
-- depends_on: {{ ref('bronze_api__FR_transaction_entries') }}
{{ config(
    materialized = 'incremental',
    unique_key = "transaction_entries_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'entry_id']
) }}

WITH bronze AS (

    SELECT
        partition_key,
        DATA,
        VALUE :STARTING_AFTER :: STRING AS starting_after,
        VALUE :LIMIT :: INTEGER AS api_limit,
        ARRAY_SIZE(
            DATA :data :: ARRAY
        ) AS entry_count,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_api__transaction_entries') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze_api__FR_transaction_entries') }}
{% endif %}
)
SELECT
    partition_key,
    entry_count,
    starting_after,
    api_limit,
    VALUE :createdAt :: timestamp_ntz AS created_at,
    VALUE :id :: STRING AS entry_id,
    INDEX :: INTEGER AS INDEX,
    VALUE :: variant AS DATA,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['partition_key']
    ) }} AS transaction_entries_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze,
    LATERAL FLATTEN(
        input => DATA :data :: ARRAY
    ) qualify(ROW_NUMBER() over (PARTITION BY entry_id
ORDER BY
    _inserted_timestamp DESC)) = 1
