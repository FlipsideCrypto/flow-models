{% set blockchain = 'flow_evm' %}
{% set full_reload_mode = var('SCORES_FULL_RELOAD_MODE', false) %}


{{ config (
    materialized = "incremental",
    unique_key = "block_date",
    incremental_strategy = "delete+insert",
    cluster_by = "block_date",
    version = 1,
    tags = ['scores']
) }}

    {% if is_incremental() %}
        {% set max_modified_timestamp_query %}
            SELECT MAX(modified_timestamp) as max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set results = run_query(max_modified_timestamp_query) %}
        {% if execute %}
            {% set max_modified_timestamp = results.columns[0].values()[0] %}
        {% else %}
            {% set max_modified_timestamp = none %}
        {% endif %}
    {% endif %}
        
    {% if execute %}    
        {{ log("==========================================", info=True) }}
        {% if is_incremental() %}
            {% set new_data_check_query %}
                SELECT COUNT(1) as cnt
                FROM {{ ref('scores__actions') }}
                WHERE modified_timestamp > '{{ max_modified_timestamp }}'
                limit 1
            {% endset %}
            {% set new_data_results = run_query(new_data_check_query) %}
            {% set new_data_count = new_data_results.columns[0].values()[0] %}
            
            {% if new_data_count > 0 %}
                {{ log("Processing action data for blockchain: " ~ blockchain ~ " modified after: " ~ max_modified_timestamp, info=True) }}
            {% else %}
                {{ log("No new action data to aggregate daily for blockchain: " ~ blockchain, info=True) }}
            {% endif %}
        {% else %}
            {{ log("Aggregating daily action data for blockchain: " ~ blockchain, info=True) }}
        {% endif %}
        {{ log("==========================================", info=True) }}
    {% endif %}

WITH actions AS (

    SELECT
        block_date,
        origin_from_address,
        origin_to_address,
        INDEX,
        block_timestamp,
        block_number,
        tx_hash,
        action_type,
        action_details,
        metric_name,
        metric_rank
    FROM
        {{ ref('scores__actions') }}
    WHERE
        1=1
        {% if is_incremental() %}
        AND modified_timestamp > '{{ max_modified_timestamp }}'
        {% endif %}
),
priorititized_txs AS (
    SELECT
        block_date,
        origin_from_address,
        origin_to_address,
        INDEX,
        block_timestamp,
        block_number,
        tx_hash,
        action_type,
        action_details,
        metric_name
    FROM
        actions
    WHERE
        action_type <> 'tx' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                metric_rank ASC
        ) = 1
),
simple_aggs AS (
    SELECT
        block_date,
        CASE
            WHEN action_type = 'contract_interaction' THEN origin_from_address
            ELSE action_details :token_from_address :: STRING
        END AS user_address,
        SUM(IFF(metric_name = 'n_bridge_in', 1, 0)) AS n_bridge_in,
        SUM(IFF(metric_name = 'n_cex_withdrawals', 1, 0)) AS n_cex_withdrawals,
        SUM(IFF(metric_name = 'n_other_defi', 1, 0)) AS n_other_defi,
        SUM(IFF(metric_name = 'n_lp_adds', 1, 0)) AS n_lp_adds,
        SUM(IFF(metric_name = 'n_swap_tx', 1, 0)) AS n_swap_tx,
        SUM(IFF(metric_name = 'n_nft_mints', 1, 0)) AS n_nft_mints,
        SUM(IFF(metric_name = 'n_nft_trades', 1, 0)) AS n_nft_trades,
        SUM(IFF(metric_name = 'n_gov_votes', 1, 0)) AS n_gov_votes,
        SUM(IFF(metric_name = 'n_stake_tx', 1, 0)) AS n_stake_tx,
        SUM(IFF(metric_name = 'n_restakes', 1, 0)) AS n_restakes
    FROM
        priorititized_txs
    GROUP BY
        ALL
),
xfer_in AS (
    SELECT
        block_date,
        action_details :token_to_address :: STRING AS user_address,
        COUNT(1) AS n_xfer_in
    FROM
        actions
    WHERE
        action_type IN (
            'erc20_transfer',
            'native_transfer'
        )
    GROUP BY
        ALL
),
xfer_out AS (
    SELECT
        block_date,
        action_details :token_from_address :: STRING AS user_address,
        COUNT(1) AS n_xfer_out
    FROM
        actions
    WHERE
        action_type IN (
            'erc20_transfer',
            'native_transfer'
        )
    GROUP BY
        ALL
),
net_token_accumulate AS (
    SELECT
        COALESCE(
            A.block_date,
            b.block_date
        ) AS block_date,
        COALESCE(
            A.user_address,
            b.user_address
        ) AS user_address,
        COALESCE(n_xfer_in / (ifnull(n_xfer_in,0) + ifnull(n_xfer_out,0)),0) AS net_token_accumulate
    FROM
        xfer_in A full
        OUTER JOIN xfer_out b
        ON A.user_address = b.user_address
        AND A.block_date = b.block_date
),
nft_collections AS (
    SELECT
        block_date,
        user_address,
        ARRAY_AGG(nft_address) AS nft_collection_addresses
    FROM
        (
            SELECT
                DISTINCT block_date,
                action_details :token_from_address :: STRING AS user_address,
                action_details: contract_address :: STRING AS nft_address
            FROM
                actions
            WHERE
                action_type IN (
                    'erc721_transfer',
                    'erc1155_transfer',
                    'erc1155_transfer_batch'
                )
            UNION
            SELECT
                DISTINCT block_date,
                action_details :token_to_address :: STRING AS user_address,
                action_details: contract_address :: STRING AS nft_address
            FROM
                actions
            WHERE
                action_type IN (
                    'erc721_transfer',
                    'erc1155_transfer',
                    'erc1155_transfer_batch'
                )
            qualify row_number() over (partition by user_address, block_date order by block_date asc) <= 1000
        )
    GROUP BY
        ALL
),
staking_validators AS (
    SELECT
        block_date,
        origin_from_address AS user_address,
        ARRAY_AGG(
            action_details :contract_address :: STRING
        ) AS validator_addresses
    FROM
        (
            select * 
            from actions
            where action_type = 'contract_interaction'
            and metric_name = 'n_stake_tx'
            qualify row_number() over (partition by origin_from_address, block_date order by block_timestamp asc) <= 1000
        )
    GROUP BY
        ALL
),
complex_txns AS (
    SELECT
        block_date,
        user_address,
        SUM(complex_tx) AS complex_tx
    FROM
        (
            SELECT
                block_date,
                origin_from_address AS user_address,
                IFF(
                    action_details :complex_tx :: BOOLEAN,
                    1,
                    0
                ) AS complex_tx,
                tx_hash
            FROM
                actions
            WHERE
                action_type = 'tx'
            UNION ALL
            SELECT
                block_date,
                COALESCE(
                    action_details: token_to_address :: STRING,
                    origin_from_address :: STRING
                ) AS user_address,
                1 AS complex_tx,
                tx_hash
            FROM
                actions
            WHERE
                metric_name = 'n_bridge_in'
        )
    GROUP BY
        ALL
),
contract_interactions AS (
    SELECT
        block_date,
        origin_from_address AS user_address,
        ARRAY_AGG(
            origin_to_address
        ) AS contract_addresses
    FROM
        (
            select * 
            from actions
            where action_type = 'tx'
            and action_details: to_address_is_contract :: BOOLEAN
            qualify row_number() over (partition by origin_from_address, block_date order by block_timestamp asc) <= 1000
        )
    GROUP BY
        ALL
),
active_day AS (
    SELECT
        block_date,
        user_address,
        MAX(active_day) = 1 AS active_day,
        SUM(active_day) AS n_txn
    FROM
        (
            SELECT
                block_date,
                origin_from_address AS user_address,
                1 AS active_day
            FROM
                actions
            WHERE
                action_type = 'tx'
            UNION ALL
            SELECT
                block_date,
                COALESCE(
                    action_details: token_to_address :: STRING,
                    origin_from_address :: STRING
                ) AS user_address,
                1 AS active_day
            FROM
                actions
            WHERE
                metric_name = 'n_bridge_in'
            UNION ALL
            SELECT
                block_date,
                COALESCE(
                    action_details: token_to_address :: STRING,
                    origin_from_address :: STRING
                ) AS user_address,
                1 AS active_day
            FROM
                actions
            WHERE
                metric_name = 'n_cex_withdrawals'
        )
    GROUP BY
        ALL
)
SELECT
    ad.block_date,
    ad.user_address,
    IFNULL(
        n_bridge_in,
        0
    ) AS n_bridge_in,
    IFNULL(
        n_cex_withdrawals,
        0
    ) AS n_cex_withdrawals,
    IFNULL(
        n_other_defi,
        0
    ) AS n_other_defi,
    IFNULL(
        n_lp_adds,
        0
    ) AS n_lp_adds,
    IFNULL(
        n_swap_tx,
        0
    ) AS n_swap_tx,
    IFNULL(
        n_nft_mints,
        0
    ) AS n_nft_mints,
    IFNULL(
        n_nft_trades,
        0
    ) AS n_nft_trades,
    IFNULL(
        n_gov_votes,
        0
    ) AS n_gov_votes,
    IFNULL(
        n_stake_tx,
        0
    ) AS n_stake_tx,
    IFNULL(
        n_restakes,
        0
    ) AS n_restakes,
    IFNULL(
        net_token_accumulate,
        0
    ) AS net_token_accumulate,
    IFNULL(nft_collection_addresses, ARRAY_CONSTRUCT()) AS nft_collection_addresses,
    IFNULL(validator_addresses, ARRAY_CONSTRUCT()) AS validator_addresses,
    IFNULL(
        complex_tx,
        0
    ) AS complex_tx,
    IFNULL(contract_addresses, ARRAY_CONSTRUCT()) AS contract_addresses,
    IFNULL(
        active_day,
        FALSE
    ) AS active_day,
    IFNULL(
        n_txn,
        0
    ) AS n_txn,
    '{{ blockchain }}' AS blockchain,
    {{ dbt_utils.generate_surrogate_key(
        ['ad.block_date','ad.user_address', "'" ~ blockchain ~ "'"]
    ) }} AS actions_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    active_day ad
    LEFT JOIN simple_aggs sa
    ON ad.user_address = sa.user_address
    AND ad.block_date = sa.block_date
    LEFT JOIN net_token_accumulate nta
    ON ad.user_address = nta.user_address
    AND ad.block_date = nta.block_date
    LEFT JOIN nft_collections nc
    ON ad.user_address = nc.user_address
    AND ad.block_date = nc.block_date
    LEFT JOIN staking_validators sv
    ON ad.user_address = sv.user_address
    AND ad.block_date = sv.block_date
    LEFT JOIN complex_txns ct
    ON ad.user_address = ct.user_address
    AND ad.block_date = ct.block_date
    LEFT JOIN contract_interactions ci
    ON ad.user_address = ci.user_address
    AND ad.block_date = ci.block_date