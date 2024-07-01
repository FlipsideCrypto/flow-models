WITH mint_events AS (
    SELECT
        MAX(block_timestamp) :: DATE AS last_mint_date
    FROM
        {{ ref('silver__nft_moments_s') }}
    WHERE
        event_contract = 'A.e4cf4bdc1751c65d.AllDay'
        AND event_type = 'MomentNFTMinted'
),
moments AS (
    SELECT
        nft_collection,
        nft_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_allday_metadata') }}
    WHERE
        _inserted_timestamp :: DATE >= (
            SELECT
                last_mint_date
            FROM
                mint_events
        )
)
SELECT
    IFF(COUNT(nft_id) > 0, TRUE, FALSE) AS recent
FROM
    moments
HAVING
    recent = FALSE
