-- depends_on: {{ ref('bronze__streamline_transaction_results') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_number >= (select min(block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ["block_number","_inserted_timestamp::date"],
    tags = ['streamline_load', 'core', 'scheduled_core']
) }}

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
{% if var('LOAD_BACKFILL', False) %}
        {{ ref('bronze__streamline_transaction_results_history') }}
        -- TODO need incremental logic of some sort probably (for those 5800 missing txs)
        -- where inserted timestamp >= max from this where network version = backfill version OR block range between root and end
{% elif var('MANUAL_FIX', False) %}
    {{ ref('bronze__streamline_fr_transaction_results') }}
    WHERE 
        _partition_by_block_id BETWEEN {{ var('RANGE_START', 0) }} AND {{ var('RANGE_END', 0) }}
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
    -- AND _partition_by_block_id > 107700000 -- march 27th 2025
    -- AND _partition_by_block_id > 108000000 -- march 28th 2025
    -- AND _partition_by_block_id > 108800000 -- april 5th 2025
{% else %}
    {{ ref('bronze__streamline_fr_transaction_results') }}
{% endif %}

{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1