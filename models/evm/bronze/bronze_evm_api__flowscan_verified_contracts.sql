{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false,
    tags = ['evm']
) }}

WITH level1 AS (

    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts/'
        ) :data AS DATA
),
level2 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level1
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level3 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level2
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level4 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level3
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level5 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level4
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level6 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level5
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level7 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level6
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level8 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level7
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level9 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level8
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
level10 AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'https://evm.flowscan.io/api/v2/smart-contracts?hash=' || DATA :next_page_params: "hash" :: STRING
        ) :data AS DATA
    FROM
        level9
    WHERE
        DATA :next_page_params: "hash" :: STRING IS NOT NULL
),
ua AS (
    SELECT
        DATA
    FROM
        level1
    UNION ALL
    SELECT
        DATA
    FROM
        level2
    UNION ALL
    SELECT
        DATA
    FROM
        level3
    UNION ALL
    SELECT
        DATA
    FROM
        level4
    UNION ALL
    SELECT
        DATA
    FROM
        level5
    UNION ALL
    SELECT
        DATA
    FROM
        level6
    UNION ALL
    SELECT
        DATA
    FROM
        level7
    UNION ALL
    SELECT
        DATA
    FROM
        level8
    UNION ALL
    SELECT
        DATA
    FROM
        level9
    UNION ALL
    SELECT
        DATA
    FROM
        level10
)
SELECT
    VALUE AS DATA,
    VALUE :address :hash :: STRING AS contract_address,
    VALUE :address :implementations AS implementations,
    VALUE :address :is_contract :: BOOLEAN AS is_contract,
    VALUE :address :is_verified :: BOOLEAN AS is_verified,
    VALUE :address :name :: STRING AS NAME,
    VALUE :address :private_tags AS private_tags,
    VALUE :verified_at :: datetime AS verified_at,
    {{ dbt_utils.generate_surrogate_key(['contract_address']) }} AS flowscan_verified_contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ua,
    LATERAL FLATTEN(
        input => DATA :items
    )
