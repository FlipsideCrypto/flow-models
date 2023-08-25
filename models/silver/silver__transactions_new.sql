-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['core']
) }}

SELECT
    block_number,
    DATA : reference_block_id :: STRING AS block_id,
    id :: STRING AS tx_id,
    DATA : gas_limit :: STRING AS gas_limit,
    DATA : payer :: STRING AS payer,
    DATA : arguments AS arguments,
    DATA : authorizers AS authorizers,
    DATA : envelope_signatures AS envelope_signatures,
    DATA : payload_signatures AS payload_signatures,
    DATA : proposal_key AS proposal_key,
    DATA : script AS script,
    _partition_by_block_id,
    _inserted_timestamp
FROM
{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_transactions') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1