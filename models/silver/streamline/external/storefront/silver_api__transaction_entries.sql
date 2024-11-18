-- depends_on: {{ ref('bronze_api__transaction_entries') }}
-- depends_on: {{ ref('bronze_api__FR_transaction_entries') }}
{{ config(
    materialized = 'incremental',
    unique_key = "transaction_entries_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'entry_id'],
    tags = ['streamline_non_core']
) }}

WITH bronze AS (

    SELECT
        partition_key,
        DATA,
        VALUE :STARTING_AFTER :: STRING AS starting_after,
        VALUE :API_LIMIT :: INTEGER AS api_limit,
        ARRAY_SIZE(
            DATA :data :: ARRAY
        ) AS entry_count,
        DATA :data [0] :id :: STRING AS first_entry_id,
        DATA :data [entry_count - 1] :id :: STRING AS last_entry_id,
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
    first_entry_id AS request_first_entry_id,
    last_entry_id AS request_last_entry_id,
    VALUE :createdAt :: timestamp_ntz AS created_at,
    VALUE :id :: STRING AS entry_id,
    INDEX :: INTEGER AS INDEX,
    VALUE :: variant AS DATA,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['entry_id', 'partition_key']
    ) }} AS transaction_entries_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze,
    LATERAL FLATTEN(
        input => DATA :data :: ARRAY
    ) 

