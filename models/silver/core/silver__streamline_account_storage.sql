-- depends_on: {{ ref('bronze__streamline_account_storage') }}
-- depends_on: {{ ref('bronze__streamline_fr_account_storage') }}
{{ config (
    materialized = "incremental",
    incremental_predicates = ["dynamic_range_predicate", "partition_key"],
    unique_key = "streamline_account_storage_id",
    cluster_by = "partition_key",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(streamline_account_storage_id)",
    tags = ['streamline_load', 'core', 'scheduled_core']
) }}

SELECT
    value:"BLOCK_HEIGHT"::INT AS block_height,
    value:"ACCOUNT_ADDRESS"::STRING AS account_address,
    value:"BLOCK_DATE"::string::Date as block_date,
    data::string as encoded_data,
    TRY_PARSE_JSON(BASE64_DECODE_STRING(encoded_data)) as decoded_data,
    file_name,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_height', 'account_address']
    ) }} AS streamline_account_storage_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_account_storage') }}
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
    {{ ref('bronze__streamline_fr_account_storage') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY streamline_account_storage_id
ORDER BY
    _inserted_timestamp DESC)) = 1
