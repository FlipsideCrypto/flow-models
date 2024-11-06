{{ config(
    materialized = 'incremental',
    unique_key = "reward_points_spend_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'entry_id']
) }}

WITH silver_responses AS (

    SELECT
        partition_key,
        entry_id,
        created_at,
        INDEX,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver_api__transaction_entries') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY entry_id
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    entry_id,
    created_at,
    DATA :amount :: NUMBER AS amount,
    DATA :direction :: STRING AS direction,
    DATA :idempotencyKey :: STRING AS idempotency_key,
    DATA :loyaltyAccountId :: STRING AS loyalty_account_id,
    DATA :loyaltyTransactionId :: STRING AS loyalty_transaction_id,
    DATA :loyaltyTransaction :: variant AS loyalty_transaction,
    DATA :loyaltyTransaction :type :: STRING AS loyalty_transaction_type,
    DATA :organizationId :: STRING AS organization_id,
    DATA :websiteId :: STRING AS website_id,
    partition_key,
    INDEX,
    OBJECT_DELETE(
        DATA,
        'amount',
        'createdAt',
        'direction',
        'id',
        'idempotencyKey',
        'loyaltyAccountId',
        'loyaltyTransaction',
        'loyaltyTransactionId',
        'type',
        'organizationId',
        'websiteId'
    ) AS DATA,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['entry_id', 'partition_key']
    ) }} AS reward_points_spend_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver_responses
