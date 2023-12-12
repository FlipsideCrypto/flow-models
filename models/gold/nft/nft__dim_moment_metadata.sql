{{ config (
    materialized = 'view',
    tags = ['nft', 'dapper', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT, ALLDAY, GOLAZOS, TOPSHOT' }} }
) }}

WITH chainwalkers AS (

    SELECT
        NULL AS nft_moment_metadata_id,
        event_contract AS nft_collection,
        nft_id,
        serial_number,
        max_mint_size,
        play_id,
        series_id,
        series_name,
        set_id,
        set_name,
        edition_id,
        tier,
        metadata,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__nft_moment_metadata_final') }}
    WHERE
        NOT (
            nft_collection = 'A.87ca73a41bb50ad5.Golazos'
            AND edition_id = 486
        )
        AND NOT (
            nft_collection = 'A.e4cf4bdc1751c65d.AllDay'
            AND edition_id = 1486
        )
),
streamline AS (
    SELECT
        nft_moment_metadata_id,
        event_contract AS nft_collection,
        nft_id,
        serial_number,
        max_mint_size,
        play_id,
        series_id,
        series_name,
        set_id,
        set_name,
        edition_id,
        tier,
        metadata,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__nft_moment_metadata_final_s') }}
    WHERE
        NOT (
            nft_collection = 'A.87ca73a41bb50ad5.Golazos'
            AND edition_id = 486
        )
        AND NOT (
            nft_collection = 'A.e4cf4bdc1751c65d.AllDay'
            AND edition_id = 1486
        )
),
FINAL AS (
    SELECT
        *
    FROM
        chainwalkers
    UNION ALL
    SELECT
        *
    FROM
        streamline
)
SELECT
    nft_collection,
    nft_id,
    serial_number,
    max_mint_size,
    play_id,
    series_id,
    series_name,
    set_id,
    set_name,
    edition_id,
    tier,
    metadata,
    COALESCE (
        nft_moment_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['nft_collection','edition_id','nft_id']
        ) }}
    ) AS dim_moment_metadata_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY nft_moment_metadata_id
        ORDER BY
            series_name IS NOT NULL DESC,
            _inserted_timestamp DESC
    ) = 1
