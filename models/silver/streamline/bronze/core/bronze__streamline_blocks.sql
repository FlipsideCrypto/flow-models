{{ config (
    materialized = 'view'
) }}

{% set 
table_names = [
    'BLOCKS_CANDIDATE_07', 'BLOCKS_CANDIDATE_08', 'BLOCKS_CANDIDATE_09', 'BLOCKS_MAINNET_01', 'BLOCKS_MAINNET_02', 'BLOCKS_MAINNET_03', 'BLOCKS_MAINNET_04', 'BLOCKS_MAINNET_05', 'BLOCKS_MAINNET_06', 'BLOCKS_MAINNET_07', 'BLOCKS_MAINNET_08', 'BLOCKS_MAINNET_09', 'BLOCKS_MAINNET_10', 'BLOCKS_MAINNET_11', 'BLOCKS_MAINNET_12', 'BLOCKS_MAINNET_13', 'BLOCKS_MAINNET_14', 'BLOCKS_MAINNET_15', 'BLOCKS_MAINNET_16', 'BLOCKS_MAINNET_17', 'BLOCKS_MAINNET_18', 'BLOCKS_MAINNET_19', 'BLOCKS_MAINNET_20', 'BLOCKS_MAINNET_21', 'BLOCKS_MAINNET_22'
]
%}
WITH 
{% for table_name in table_names %}
    meta_{{ table_name }} AS (
        SELECT
            last_modified AS _inserted_timestamp,
            file_name,
            CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER) AS _partition_by_block_id
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", table_name ) }}')
                ) A
        ),
        {{ table_name }} AS (
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
                {{ source(
                    "bronze_streamline",
                    table_name
                ) }}
                s
                JOIN meta_{{ table_name }}
                b
                ON b.file_name = metadata$filename
                AND b._partition_by_block_id = s._partition_by_block_id
            WHERE
                b._partition_by_block_id = s._partition_by_block_id
        ),
{% endfor %}

FINAL AS ({% for table_name in table_names %}
    SELECT
        *
    FROM
        {{ table_name }}

    {% if not loop.last %}
            UNION ALL
    {% endif %}
    {% endfor %}
)
SELECT
    *
FROM
    FINAL
