-- depends_on: {{ ref('scores__actions_daily') }}
-- depends_on: {{ ref('core_evm__dim_labels') }}
-- depends_on: {{ ref('core_evm__dim_contracts') }}

{% set blockchain = 'flow_evm' %}
{% set full_reload_mode = var('SCORES_FULL_RELOAD_MODE', false) %}
{% set score_date_limit = var('SCORES_DATE_LIMIT', 30) %}


{{ config (
    materialized = "incremental",
    unique_key = "score_date",
    incremental_strategy = "delete+insert",
    cluster_by = "score_date",
    version = 1,
    full_refresh = false,
    tags = ['scores']
) }}

    {% set score_dates_query %}
        SELECT block_date as score_date
        FROM {{ ref('scores__target_days') }}
        
        {% if not full_reload_mode %}
            WHERE score_date > dateadd('day', -120, sysdate())
        {% endif %}

        {% if is_incremental() %}
            EXCEPT 
            SELECT DISTINCT score_date 
            FROM {{ this }}
            {% if not full_reload_mode %}
                WHERE score_date > dateadd('day', -120, sysdate())
            {% endif %}
        {% endif %}

        ORDER BY score_date ASC
        {% if score_date_limit %}
            LIMIT {{ score_date_limit }}
        {% endif %}
    {% endset %}

    {% set score_dates = run_query(score_dates_query) %}

    {% if execute %}
        {% set score_dates_list = score_dates.columns[0].values() %}
    {% else %}
        {% set score_dates_list = [] %}
    {% endif %}

    {% if execute %}
         {% if score_dates_list|length > 0 %}
            {{ log("==========================================", info=True) }}
            {% if score_dates_list|length == 1 %}
                {{ log("Calculating action totals for blockchain: " ~ blockchain, info=True) }}
                {{ log("For score date: " ~ score_dates_list[0], info=True) }}
            {% else %}
                {{ log("Calculating action totals for blockchain: " ~ blockchain, info=True) }}
                {{ log("For score dates: " ~ score_dates_list|join(', '), info=True) }}
            {% endif %}
            {{ log("==========================================", info=True) }}
        {% else %}
            {{ log("==========================================", info=True) }}
            {{ log("No action totals to calculate for blockchain: " ~ blockchain, info=True) }}
            {{ log("==========================================", info=True) }}
        {% endif %} 
    {% endif %} 

    {% if score_dates_list|length > 0 %}
        WITH combined_results AS (
            {% for score_date in score_dates_list %}
                SELECT
                    user_address,
                    n_complex_txn,
                    n_contracts,
                    n_days_active,
                    n_txn,
                    n_bridge_in,
                    n_cex_withdrawals,
                    net_token_accumulate,
                    n_other_defi,
                    n_lp_adds,
                    n_swap_tx,
                    n_nft_collections,
                    n_nft_mints,
                    n_nft_trades,
                    n_gov_votes,
                    n_stake_tx,
                    n_restakes,
                    n_validators,
                    CURRENT_TIMESTAMP AS calculation_time,
                    CAST('{{ score_date }}' AS DATE) AS score_date,
                    '{{ blockchain }}' AS blockchain,
                    {{ dbt_utils.generate_surrogate_key(['user_address', "'" ~ blockchain ~ "'", "'" ~ score_date ~ "'"]) }} AS actions_agg_id,
                    '{{ model.config.version }}' AS score_version,
                    SYSDATE() AS inserted_timestamp,
                    SYSDATE() AS modified_timestamp,
                    '{{ invocation_id }}' AS _invocation_id
                FROM
                    (
                        SELECT
                            user_address,
                            SUM(n_bridge_in) AS n_bridge_in,
                            SUM(n_cex_withdrawals) AS n_cex_withdrawals,
                            SUM(n_other_defi) AS n_other_defi,
                            SUM(n_lp_adds) AS n_lp_adds,
                            SUM(n_swap_tx) AS n_swap_tx,
                            SUM(n_nft_mints) AS n_nft_mints,
                            SUM(n_nft_trades) AS n_nft_trades,
                            SUM(n_gov_votes) AS n_gov_votes,
                            SUM(n_stake_tx) AS n_stake_tx,
                            SUM(n_restakes) AS n_restakes,
                            SUM(net_token_accumulate) AS net_token_accumulate,
                            SUM(n_txn) AS n_txn,
                            SUM(IFF(active_day, 1, 0)) AS n_days_active,
                            SUM(complex_tx) AS n_complex_txn,
                            ARRAY_SIZE(ARRAY_COMPACT(ARRAY_DISTINCT(ARRAY_UNION_AGG(validator_addresses)))) AS n_validators,
                            ARRAY_SIZE(ARRAY_COMPACT(ARRAY_DISTINCT(ARRAY_UNION_AGG(contract_addresses)))) AS n_contracts,
                            ARRAY_SIZE(ARRAY_COMPACT(ARRAY_DISTINCT(ARRAY_UNION_AGG(nft_collection_addresses)))) AS n_nft_collections
                        FROM
                            {{ ref('scores__actions_daily') }} a 
                        LEFT JOIN {{ ref('core_evm__dim_labels') }} b
                            ON a.user_address = b.address
                        LEFT JOIN {{ ref('core_evm__dim_contracts') }} c
                            ON a.user_address = c.address
                        WHERE
                            b.address IS NULL
                            AND c.address IS NULL
                            AND a.block_date BETWEEN DATEADD('day', -90, '{{ score_date }}') AND '{{ score_date }}' :: DATE
                        GROUP BY
                            user_address
                    )
                {% if not loop.last %}
                    UNION ALL
                {% endif %}
            {% endfor %}
        )

        SELECT * FROM combined_results

    {% else %}
        -- Return an empty result set with the correct schema
        SELECT
            CAST(NULL AS STRING) AS user_address,
            CAST(NULL AS INTEGER) AS n_complex_txn,
            CAST(NULL AS INTEGER) AS n_contracts,
            CAST(NULL AS INTEGER) AS n_days_active,
            CAST(NULL AS INTEGER) AS n_txn,
            CAST(NULL AS INTEGER) AS n_bridge_in,
            CAST(NULL AS INTEGER) AS n_cex_withdrawals,
            CAST(NULL AS FLOAT) AS net_token_accumulate,
            CAST(NULL AS INTEGER) AS n_other_defi,
            CAST(NULL AS INTEGER) AS n_lp_adds,
            CAST(NULL AS INTEGER) AS n_swap_tx,
            CAST(NULL AS INTEGER) AS n_nft_collections,
            CAST(NULL AS INTEGER) AS n_nft_mints,
            CAST(NULL AS INTEGER) AS n_nft_trades,
            CAST(NULL AS INTEGER) AS n_gov_votes,
            CAST(NULL AS INTEGER) AS n_stake_tx,
            CAST(NULL AS INTEGER) AS n_restakes,
            CAST(NULL AS INTEGER) AS n_validators,
            CAST(NULL AS TIMESTAMP) AS calculation_time,
            CAST(NULL AS DATE) AS score_date,
            CAST(NULL AS STRING) AS blockchain,
            CAST(NULL AS STRING) AS actions_agg_id,
            CAST(NULL AS STRING) AS score_version,
            CAST(NULL AS TIMESTAMP) AS inserted_timestamp,
            CAST(NULL AS TIMESTAMP) AS modified_timestamp,
            CAST(NULL AS STRING) AS _invocation_id
        WHERE 1 = 0
    {% endif %}