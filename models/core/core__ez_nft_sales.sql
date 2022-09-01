{{ config(
    materialized = 'view',
    tags = ['nft', 'ez']
) }}

WITH silver_nfts AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        block_timestamp >= '2022-04-20'
),
gold_nfts AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        marketplace,
        nft_collection,
        nft_id,
        buyer,
        seller,
        price,
        currency,
        tx_succeeded,
        tokenflow,
        counterparties
    FROM
        silver_nfts
    WHERE
        tx_id NOT IN (
            '8620792f30d607a35eb5a7ffe6ea2a088d448f1b706e8585ca8ae8697655e6fa',
            '489584d6def5e583d79864046e47b37888c935bfad6b2b17ac67be4b04209f55',
            '0934ec1c9bf6c52cbd11e3e4e39154d147af06c95a8bdbc3936839ed19665090',
            '69d577729d6abf7b3e71e91b0f8df737f044f5cec40b2872376b80ddb934a7e2',
            '43d7cefcdb35aee175b8c573a247bcfa029a82db7f99265d0b14fbb6c9b63360',
            '507fc7eda60d5f4706891d3f48be70f20c6c115ee81e419dc9daa673e87c77c7',
            'f9c0de48de30624b2f42924f69b8e9ef36fb1995ad37921534131b2f28888027',
            '4b98e11f4482231c7d41c921874c2c0dfacdb0b537020e7e4030d683aebbd98a',
            '6dc3a5bb564d1935ccea5213da686d4f367ffb4a21361e0badc841cb84e2d5dc',
            '614d1018d5e93711f50dbbeb9b779ba3b7e8577e08c34d69c905cef45239c03e'
        )
)
SELECT
    *
FROM
    gold_nfts
