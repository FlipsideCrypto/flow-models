-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['streamline_load', 'core', 'scheduled_core']
) }}

SELECT
    block_number,
    DATA: reference_block_id :: STRING AS block_id,
    id AS tx_id,
    DATA: gas_limit :: NUMBER AS gas_limit,
    DATA: payer :: STRING AS payer,
    DATA: arguments :: ARRAY AS arguments,
    DATA: authorizers :: ARRAY AS authorizers,
    DATA: envelope_signatures :: ARRAY AS envelope_signatures,
    DATA: payload_signatures :: ARRAY AS payload_signatures,
    DATA: proposal_key :: variant AS proposal_key,
    DATA: script :: STRING AS script,
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

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
