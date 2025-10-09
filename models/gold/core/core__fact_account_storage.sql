{{ config(
    materialized = 'incremental',
    cluster_by = ['block_date'],
    unique_key = "fact_account_storage_id",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(fact_account_storage_id, account_address)",
    tags = ['scheduled_non_core']
) }}

SELECT
    block_height,
    account_address,
    block_date,
    encoded_data,
    decoded_data,
    decoded_data:value[0]:value::NUMBER as storage_used,
    decoded_data:value[1]:value::NUMBER as storage_capacity,
    streamline_account_storage_id as fact_account_storage_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__account_storage') }}
WHERE 1=1
and encoded_data is not null
{% if is_incremental() %}
    AND modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}
