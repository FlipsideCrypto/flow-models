-- depends_on: {{ ref('bronze__streamline_transaction_results') }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
-- depends_on: {{ ref('bronze__streamline_fr_transaction_results') }}
-- depends_on: {{ ref('bronze__streamline_fr_transactions') }}

{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_number >= (select min(block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ["block_number","_inserted_timestamp::date"],
    tags = ['streamline_load', 'core', 'scheduled_core']
) }}

WITH pending_txs AS (
    SELECT tx_id FROM (VALUES
        ('b2cf69ce1449679be0166f1a44da6c6570adb1414ec9816b4afbb91dae1f03bb'),
        ('a3e3c9ad914a37dd875189ff4c91e2e5a03d4155735b4a4a3028ce6f966dab98'),
        ('c096c4c61a2e0943b1b55e1eb21e513dda42927f8024fc0898471e6830268721'),
        ('ba003ee793ddd7192882f5104678f6b75a9b812355031fc65b003ab4d6e4205b'),
        ('84816e6c3789eee14bae5f6785e0c0d159203cd70c6cd8f803f5b0be50b06770'),
        ('b4e2fcb90c093fb38b11d88888133d1b6cf428d7fdf229bbcae4941d8f4a8c6c'),
        ('19355c74bfa38525a3cf82065bb767982f19d1e9fb8fc92132dfcb1fe076967f'),
        ('2f78ac9a26dd4efaf93c73a6c3a3ac042f872f78e82e67c4895621c75201477f'),
        ('0db398d0d84f27384f2fe62005c44646548afa34c37281f5a809c58ead3c97d6'),
        ('34910aafee924d7e8c82e61e824712c8c0be7de094afdc2cf30226631f31f9cd'),
        ('ce1d6b46eea8453ab2863fb54f6bd1bfde0a09c718b7ca6a4cbede2a089c88bb'),
        ('49b0de9199b80f71f1f430bdd9ab533e58009b1435f035414d5a745f56f09520'),
        ('78c0e8d902eebe13ad3d3ff98f51c84524c18c0f5b18240d18dbf66059e3c628'),
        ('b71c82cd10d9b79f8b2d70c13d42a3b0e28f0ad9013b19bb418b6ad05071ce27'),
        ('a21a041cdd7072c84353eac9c941d1067fa3529cc8bf936240bb4d6e50d7a7df'),
        ('e9a7acd9920f5cd6d1172335bf582cd65314fc75c74bc764998a4974f932d5db'),
        ('bbeb22462022f6b2e36df567633db769cbff08ab15afc7addcab5626267fc822'),
        ('fcf6939858336a81c8596b388b308d18a9d5026a1192b96544bf8ecbcdf89e1f'),
        ('369e20c21a79a90227904944249f1593d4a976c3a7abab899f1c57fa2ce2081f'),
        ('e3cd5672217f61d0f25d2a610f6b72d50fc345e972eaf3ceef86d943bfeceb7f'),
        ('9d8533baaa338eae2d265e74c2a3b75e9fd1bef7c269690e14000d2cba705cf6'),
        ('31df9396feb84440531ecd54e7df8fc0d52e09ac2aacb58fb70b15eb2edf5cf5'),
        ('a1ca5c329bca0ab581b2aa2a5aa6b1d95da3f63680e558bfd40171fc684e6cd7'),
        ('cd77bfd4a970b30dfe73c801d22f707495703207cf7ff657498d6067f2146560'),
        ('daafac44cadcc7246f2a6dd54a3cc1c45a2bc5bfdd73e81b573984d5cd0b6a78'),
        ('edf558833c45201db636a2cdfdb31b3ee6deb2de247f50efbd01bad3e4665d14')
    ) AS t(tx_id)
)

SELECT
    block_number,
    id AS tx_id,
    DATA :error_message :: STRING AS error_message,
    DATA :events :: ARRAY AS events,
    DATA :status :: INT AS status,
    DATA :status_code :: INT AS status_code,
    _partition_by_block_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }} AS tx_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id  
FROM

{% if var('LOAD_BACKFILL', False) %}
        {{ ref('bronze__streamline_transaction_results_history') }}
        -- TODO need incremental logic of some sort probably (for those 5800 missing txs)
        -- where inserted timestamp >= max from this where network version = backfill version OR block range between root and end
{% elif var('MANUAL_FIX', False) %}
    {{ ref('bronze__streamline_fr_transaction_results') }}
    WHERE 
        _partition_by_block_id BETWEEN {{ var('RANGE_START', 0) }} AND {{ var('RANGE_END', 0) }}
        and tx_id in (select tx_id from pending_txs)
{% else %}

{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_results') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND tx_id in (select tx_id from pending_txs)
{% else %}
    {{ ref('bronze__streamline_fr_transaction_results') }}
    WHERE tx_id in (select tx_id from pending_txs)
{% endif %}

{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
