{{ config (
    materialized = 'view',
    tags = ['nft', 'dapper', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT, ALLDAY, GOLAZOS, TOPSHOT' }} }
) }}

SELECT
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
    COALESCE (
        nft_moment_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['nft_collection','edition_id','nft_id']
        ) }}
    ) AS dim_moment_metadata_id,
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

qualify ROW_NUMBER() over (
    PARTITION BY dim_moment_metadata_id
    ORDER BY
        inserted_timestamp DESC
) = 1
