{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['block_timestamp::date', 'modified_timestamp::date'],
    unique_key = "topshot_buyback_id",
    tags = ['nft', 'topshot', 'scheduled'],
    meta = { 'database_tags': { 'table': { 'PURPOSE': 'NFT, TOPSHOT' } } }
) }}

WITH flowty_sales AS (
  SELECT
    tx_id AS tx_id,
    block_timestamp,
    EVENT_DATA:buyer :: string AS buyer,
    event_data:storefrontAddress :: string AS seller,
    CAST(EVENT_DATA:"salePrice" AS DECIMAL(18, 2)) AS price,
    event_data:salePaymentVaultType as currency, 
    'A.3cdbb3d569211ff3.NFTStorefrontV2' as marketplace,
    'FLOWTY' as sale_type,
    EVENT_DATA:nftType :: string as nft_collection,
    EVENT_DATA:nftID :: string as nft_id,
    modified_timestamp
  FROM
    {{ ref('core__fact_events') }} AS events
  WHERE
    EVENT_CONTRACT IN ('A.3cdbb3d569211ff3.NFTStorefrontV2')
    AND EVENT_TYPE = 'ListingCompleted'
    AND TX_SUCCEEDED = TRUE
    AND EVENT_DATA:purchased :: string = 'true'
    AND CAST(EVENT_DATA:"salePrice" AS DECIMAL(18, 2)) > 0
    AND EVENT_DATA:nftType :: string = 'A.0b2a3299cc857e29.TopShot'
    
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),

all_sales AS (
    SELECT
        tx_id AS tx_id,
        CONVERT_TIMEZONE('UTC', 'America/New_York', BLOCK_TIMESTAMP) as block_timestamp,
        buyer AS buyer,
        seller AS seller,
        price AS price,
        nft_collection AS nft_collection,
        nft_id AS nft_id,
        modified_timestamp
    FROM {{ ref('nft__ez_nft_sales') }} AS sales
    WHERE nft_collection = 'A.0b2a3299cc857e29.TopShot'
        AND TX_SUCCEEDED = TRUE
        
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    
    UNION ALL
    
    SELECT
        tx_id AS tx_id,
        block_timestamp AS block_timestamp,
        buyer AS buyer,
        seller AS seller,
        price AS price,
        nft_collection AS nft_collection,
        nft_id AS nft_id,
        modified_timestamp
    FROM flowty_sales AS fs
),

    sales_with_running_total AS (
        SELECT
            block_timestamp AS block_timestamp,
            DATE_TRUNC('DAY', block_timestamp) as block_day,
            tx_id AS tx_id,
            nft_id AS nft_id,
            buyer AS buyer,
            seller AS seller,
            price AS price,
            1 as sale_count,
            ROW_NUMBER() OVER (PARTITION BY tx_id, nft_id ORDER BY block_timestamp) as rn,
            SUM(price) OVER (ORDER BY block_timestamp ROWS UNBOUNDED PRECEDING) as running_total,
            modified_timestamp
        FROM all_sales AS asales
        WHERE buyer = '0xe1f2a091f7bb5245'  -- Filtering for TopShot buyback wallet
    )

    SELECT
        s.block_timestamp AS block_timestamp,
        s.block_height AS block_height,
        s.tx_id AS tx_id,
        s.nft_id AS nft_id,
        COALESCE(ts.player, mm.metadata:player::string) as player,
        COALESCE(ts.team, mm.metadata:team::string) as team,
        COALESCE(ts.season, mm.metadata:season::string) as season,
        COALESCE(ts.set_name, mm.set_name) as set_name,
        s.buyer AS buyer,
        s.seller AS seller,
        s.price AS price,
        s.sale_count as sale,
        s.running_total as total,
        CONCAT($$https://nbatopshot.com/moment/$$, s.nft_id) AS URL,
        {{ dbt_utils.generate_surrogate_key(['s.tx_id', 's.nft_id']) }} AS topshot_buyback_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM sales_with_running_total s
    LEFT JOIN {{ ref('nft__dim_topshot_metadata') }} ts
        ON s.nft_id = ts.nft_id
    LEFT JOIN {{ ref('nft__dim_moment_metadata') }} mm
        ON s.nft_id = mm.nft_id
        AND ts.player IS NULL -- Only pull from moment_metadata if topshot_metadata is empty
    WHERE s.rn = 1  -- Deduplicate if needed