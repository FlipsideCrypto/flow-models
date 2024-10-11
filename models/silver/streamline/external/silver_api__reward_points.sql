-- depends_on: {{ ref('bronze_api__reward_points') }}
-- depends_on: {{ ref('bronze_api__FR_reward_points') }}
{{ config(
    materialized = 'incremental',
    unique_key = "reward_points_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'address']
) }}

SELECT
    partition_key,
    VALUE :ADDRESS :: STRING as address,
    to_timestamp(partition_key) :: DATE AS request_date,
    DATA :boxes :: NUMBER as boxes,
    DATA :boxes_opened :: NUMBER as boxes_opened,
    DATA :eth_address :: STRING as eth_address,
    DATA :keys :: NUMBER as keys,
    DATA :points :: NUMBER as points,
    {{ dbt_utils.generate_surrogate_key(
        ['address', 'partition_key']
    ) }} AS reward_points_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_api__reward_points') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze_api__FR_reward_points') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY reward_points_id
ORDER BY
    _inserted_timestamp DESC)) = 1
