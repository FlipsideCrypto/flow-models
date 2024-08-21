WITH mint_events AS (
    SELECT
        MAX(block_timestamp) :: DATE AS last_mint_date
    FROM
        {{ ref('silver__nft_moments_s') }}
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'
        AND event_type = 'MomentMinted'
),
moments AS (
    SELECT
        nft_collection,
        nft_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_topshot_metadata') }}
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
