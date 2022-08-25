{{ config(
    materialized = 'view',
) }}

WITH mints AS (

    SELECT
        event_contract,
        event_data :id :: STRING AS moment_id
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_contract = 'A.e4cf4bdc1751c65d.AllDay'
        AND event_type = 'MomentNFTMinted'
),
sales AS (
    SELECT
        nft_collection AS event_contract,
        nft_id as moment_id
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        nft_collection = 'A.e4cf4bdc1751c65d.AllDay'
),
all_day_ids AS (
    SELECT
        *
    FROM
        mints
    UNION
    SELECT
        *
    FROM
        sales
)
SELECT
    *
FROM
    all_day_ids
EXCEPT 
select 
    contract as event_contract,
    id AS moment_id
from {{ source('flow_external', 'moments_metadata_api')}}
where contract = 'A.e4cf4bdc1751c65d.AllDay'
