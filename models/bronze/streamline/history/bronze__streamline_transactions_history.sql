{{ config (
    materialized = 'view'
) }}

{% set history_model = "TRANSACTIONS_" ~ var('LOAD_BACKFILL_VERSION') %}

WITH meta AS (
    SELECT
        registered_on AS _inserted_timestamp,
        file_name,
        CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER) AS _partition_by_block_id
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", history_model ) }}'
            )
        ) A
)
SELECT
    block_number,
    id,
    DATA,
    _inserted_timestamp,
    MD5(
        CAST(
            COALESCE(CAST(block_number AS text), '' :: STRING) AS text
        )
    ) AS _fsc_id,
    s._partition_by_block_id,
    s.value AS VALUE
FROM
    {{ source("bronze_streamline", history_model ) }} s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b._partition_by_block_id = s._partition_by_block_id
WHERE
    b._partition_by_block_id = s._partition_by_block_id
