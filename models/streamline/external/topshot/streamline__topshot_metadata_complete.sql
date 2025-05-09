-- depends_on: {{ ref('bronze__streamline_topshot_metadata') }}
-- depends_on: {{ ref('bronze__streamline_topshot_metadata_FR') }}
{{ config (
    materialized = "incremental",
    unique_key = "topshot_metadata_complete_id",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_complete']
) }}

SELECT
    VALUE :CONTRACT :: STRING AS event_contract,
    VALUE :id :: STRING AS moment_id,
    DATA :errors :extensions :error_reason :: STRING AS error_reason,
    partition_key :: STRING AS partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['event_contract','moment_id']
    ) }} AS topshot_metadata_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_topshot_metadata') }}
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
    {{ ref('bronze__streamline_topshot_metadata_FR') }}
{% endif %}

{% if is_incremental() %}
{% else %}
    UNION ALL
    SELECT
        contract AS event_contract,
        id AS moment_id,
        CASE
            WHEN len(
                DATA :getMintedMoment
            ) IS NULL THEN 'null data'
        END error_reason,
        to_char(TO_TIMESTAMP_NTZ(SYSDATE()), 'YYYYMMDD') AS partition_key,
        _INSERTED_DATE AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['event_contract','moment_id']
        ) }} AS topshot_metadata_complete_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ source(
            'bronze_streamline',
            'moments_minted_metadata_api'
        ) }}
    {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY topshot_metadata_complete_id
ORDER BY
    _inserted_timestamp DESC)) = 1
