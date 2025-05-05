-- depends_on: {{ ref('bronze_api__reward_points') }}
{{ config(
    materialized = 'incremental',
    unique_key = "reward_points_complete_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['request_date :: DATE', 'address'],
    tags = ['streamline_non_core']
) }}
SELECT
    partition_key,
    VALUE :ADDRESS :: STRING as address,
    to_timestamp(partition_key) :: DATE AS request_date,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    {{ dbt_utils.generate_surrogate_key(
        ['address', 'partition_key']
    ) }} AS reward_points_complete_id
FROM
    {{ ref('bronze_api__reward_points') }}
WHERE 
    TYPEOF(DATA) != 'NULL_VALUE'

{% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
