WITH moments AS (
    SELECT
        nft_collection,
        nft_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_topshot_metadata') }}
    WHERE
        _inserted_timestamp :: DATE >= CURRENT_DATE - 1
)
SELECT
    IFF(COUNT(nft_id) > 0, TRUE, FALSE) AS recent
FROM
    moments
HAVING
    recent = FALSE
