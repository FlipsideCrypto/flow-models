{{ config (
    materialized = 'view'
) }}

WITH meta AS (
        SELECT
            last_modified AS _inserted_timestamp,
            file_name,
            CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER) AS _partition_by_block_id
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", "blocks") }}')
                ) A
            )
        SELECT
            block_number,
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
            streamline.FLOW_DEV.blocks
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b._partition_by_block_id = s._partition_by_block_id
        WHERE
            b._partition_by_block_id = s._partition_by_block_id

