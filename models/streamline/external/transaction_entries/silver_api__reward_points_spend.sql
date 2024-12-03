{{ config(
    materialized = 'incremental',
    unique_key = "entry_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE'],
    post_hook = [ "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(user_wallet_address)" ],
    tags = ['rewards_points_spend']
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

    DATA :direction :: STRING AS direction,
    DATA :amount :: NUMBER AS amount,
    DATA :loyaltyAccountStartAmount :: NUMBER AS amount_start,
    DATA :loyaltyAccountEndAmount :: NUMBER AS amount_end,

    DATA :idempotencyKey :: STRING AS idempotency_key,
    DATA :organizationId :: STRING AS organization_id,
    DATA :websiteId :: STRING AS website_id,

    DATA :loyaltyAccountId :: STRING AS account_id,
    DATA :loyaltyAccount :user :id :: STRING AS user_id,
    DATA :loyaltyAccount :user :walletAddress :: STRING AS user_wallet_address,

    DATA :loyaltyTransactionId :: STRING AS transaction_id,
    DATA :loyaltyTransaction :description :: STRING AS transaction_description,
    DATA :loyaltyTransaction :type :: STRING AS transaction_type,

    DATA :loyaltyTransaction :loyaltyRule :id :: STRING AS rule_id,
    DATA :loyaltyTransaction :loyaltyRule :type :: STRING AS rule_type,
    DATA :loyaltyTransaction :loyaltyRule :name :: STRING AS rule_name,
    DATA :loyaltyTransaction :loyaltyRule :description :: STRING AS rule_description,
    DATA :loyaltyTransaction :loyaltyRule :metadata :: variant AS rule_metadata,

    OBJECT_DELETE(
        DATA,
        'amount',
        'createdAt',
        'direction',
        'idempotencyKey',
        'loyaltyAccount',
        'loyaltyAccountId',
        'loyaltyAccountEndAmount',
        'loyaltyAccountStartAmount',
        'loyaltyTransactionId',
        'organizationId',
        'websiteId'
    ) AS DATA,
    partition_key,
    INDEX,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['entry_id', 'partition_key']
    ) }} AS reward_points_spend_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver_responses
