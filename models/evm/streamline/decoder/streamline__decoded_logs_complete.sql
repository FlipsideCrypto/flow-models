-- depends_on: {{ ref('bronze__decoded_logs') }}
{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_log_id)",
    tags = ['streamline_decoded_logs_complete']
) }}

{# Main query starts here #}
SELECT
    block_number,
    file_name,
    id AS _log_id,
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS complete_decoded_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__decoded_logs') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__decoded_logs_fr') }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY _inserted_timestamp DESC)) = 1
