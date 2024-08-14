-- depends_on: {{ ref('bronze__streamline_testnet_transactions') }}
-- depends_on: {{ ref('bronze__streamline_fr_testnet_transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = "testnet_transactions_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['testnet']
) }}

SELECT
    block_number,
    VALUE :id :: STRING AS transaction_id,
    DATA :arguments :: ARRAY AS arguments,
    DATA :authorizers :: ARRAY AS authorizers,
    DATA :envelope_signatures :: ARRAY AS envelope_signatures,
    DATA: gas_limit :: INT AS gas_limit,
    DATA :payer :: STRING AS payer,
    DATA :payload_signatures :: ARRAY AS payload_signatures,
    DATA :proposal_key :: variant AS proposal_key,
    DATA :reference_block_id :: STRING AS reference_block_id,
    DATA :script :: STRING AS script,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['VALUE :id ::STRING']
    ) }} AS testnet_transactions_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_testnet_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_testnet_transactions') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY testnet_transactions_id
ORDER BY
    _inserted_timestamp DESC)) = 1
