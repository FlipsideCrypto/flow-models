SELECT
    DATEDIFF(
        SECOND,
        A.block_timestamp,
        b.block_timestamp
    ) AS avg_time_diff,
    b.block_height AS bheight,
    b.block_timestamp AS btime,
    A.*
FROM
    {{ ref('silver__blocks') }} A,
    {{ ref('silver__blocks') }}
    b
WHERE
    A.block_height = b.block_height -1
    AND avg_time_diff < 0
