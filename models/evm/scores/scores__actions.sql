-- depends_on: {{ ref('scores__target_days') }}

{% set blockchain = 'flow_evm' %}
{% set full_reload_mode = var('SCORES_FULL_RELOAD_MODE', false) %}


{{ config (
    materialized = "incremental",
    unique_key = "block_date",
    incremental_strategy = "delete+insert",
    cluster_by = "block_date",
    full_refresh = false,
    tags = ['scores']
) }}

{% if is_incremental() %}
    {% set max_block_date_query %}
        WITH target_days AS (
            SELECT block_date 
            FROM {{ ref('scores__target_days') }}
            {% if not full_reload_mode %}
                WHERE block_date > dateadd('day', -120, sysdate())
            {% endif %}
        ),
        processed_days AS (
            SELECT DISTINCT block_date
            FROM {{ this }}
            {% if not full_reload_mode %}
                WHERE block_date > dateadd('day', -120, sysdate())
            {% endif %}
        ),
        unprocessed_days AS (
            SELECT block_date
            FROM target_days
            EXCEPT
            SELECT block_date
            FROM processed_days
        )
        SELECT block_date
        FROM unprocessed_days
        {% endset %}

        {% set results = run_query(max_block_date_query) %}
        {% if execute %}
            {% set block_dates = results.columns[0].values() %}
            {% if block_dates|length > 0 %}
                {{ log("==========================================", info=True) }}
                {{ log("Loading action data for blockchain: " ~ blockchain, info=True) }}
                {{ log("For block dates: " ~ block_dates|join(', '), info=True) }}
                {{ log("==========================================", info=True) }}
            {% else %}
                {{ log("==========================================", info=True) }}
                {{ log("No new action data to process for blockchain: " ~ blockchain, info=True) }}
                {{ log("==========================================", info=True) }}
            {% endif %}
        {% else %}
            {% set block_dates = [] %}
        {% endif %}
    {% endif %}

{% if (is_incremental() and block_dates|length > 0) or (not is_incremental()) %}
    WITH txs AS (
        SELECT
            block_timestamp :: DATE AS block_date,
            from_address,
            to_address,
            input_data <> '0x'
            AND LEFT(input_data, 10) <> '0xa9059cbb' AS complex_tx,
            input_data <> '0x' AS to_address_is_contract,
            block_timestamp,
            tx_hash,
            block_number
        FROM
            {{ ref('core_evm__fact_transactions') }} t

        {% if is_incremental() %}
            INNER JOIN (
                {% if block_dates|length == 1 %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                {% else %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                    {% for date in block_dates[1:] %}
                    UNION ALL SELECT CAST('{{ date }}' AS DATE)
                    {% endfor %}
                {% endif %}
            ) b ON t.block_timestamp::date = b.block_date
        {% endif %}

        WHERE 
            tx_succeeded

        {% if is_incremental() %}
            AND 1=1
        {% else %}
            AND block_timestamp :: DATE < (SELECT MAX(block_timestamp)::DATE FROM {{ ref('core_evm__fact_transactions') }})
        {% endif %}
    ),
    raw_logs AS (
        SELECT
            block_timestamp :: DATE AS block_date,
            origin_from_address,
            origin_to_address,
            event_index,
            contract_address,
            topics[0] :: STRING AS event_sig,
            topics,
            data,
            block_timestamp,
            tx_hash,
            block_number
        FROM
            {{ ref('core_evm__fact_event_logs') }} l
        JOIN txs USING (block_number, tx_hash)

        {% if is_incremental() %}
            INNER JOIN (
                {% if block_dates|length == 1 %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                {% else %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                    {% for date in block_dates[1:] %}
                    UNION ALL SELECT CAST('{{ date }}' AS DATE)
                    {% endfor %}
                {% endif %}
            ) b ON l.block_timestamp::date = b.block_date
        {% endif %}

        WHERE
            
        {% if is_incremental() %}
            1=1
        {% else %}
            l.block_timestamp :: DATE < (SELECT MAX(block_timestamp)::DATE FROM {{ ref('core_evm__fact_transactions') }})
        {% endif %}
    ),
    decoded_event_logs AS (
        SELECT
            event_index,
            contract_address,
            event_name,
            block_timestamp,
            tx_hash,
            block_number
        FROM
            {{ ref('core_evm__ez_decoded_event_logs') }} dl
        JOIN txs USING (block_number, tx_hash)

        {% if is_incremental() %}
            INNER JOIN (
                {% if block_dates|length == 1 %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                {% else %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                    {% for date in block_dates[1:] %}
                    UNION ALL SELECT CAST('{{ date }}' AS DATE)
                    {% endfor %}
                {% endif %}
            ) b ON dl.block_timestamp::date = b.block_date
        {% endif %}

        WHERE
            
        {% if is_incremental() %}
            1=1
        {% else %}
            dl.block_timestamp :: DATE < (SELECT MAX(block_timestamp)::DATE FROM {{ ref('core_evm__fact_transactions') }})
        {% endif %}
    ),
    native_transfers AS (
        SELECT
            from_address,
            to_address,
            value,
            block_timestamp,
            tx_hash,
            block_number,
            trace_index
        FROM
            {{ ref('core_evm__fact_traces') }} tr
        JOIN txs USING (block_number, tx_hash)

        {% if is_incremental() %}
            INNER JOIN (
                {% if block_dates|length == 1 %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                {% else %}
                    SELECT CAST('{{ block_dates[0] }}' AS DATE) AS block_date
                    {% for date in block_dates[1:] %}
                    UNION ALL SELECT CAST('{{ date }}' AS DATE)
                    {% endfor %}
                {% endif %}
            ) b ON tr.block_timestamp::date = b.block_date
        {% endif %}

        WHERE

        {% if is_incremental() %}
            1=1
        {% else %}
            tr.block_timestamp :: DATE < (SELECT MAX(block_timestamp)::DATE FROM {{ ref('core_evm__fact_transactions') }})
        {% endif %}
        AND value > 0
        AND trace_succeeded
    ),
    event_names AS (
        SELECT
            block_date,
            origin_from_address,
            origin_to_address,
            contract_address,
            event_sig,
            COALESCE(d.event_name, e.event_name) AS event_name,
            event_index,
            topics,
            data,
            block_timestamp,
            tx_hash,
            block_number
        FROM
            raw_logs
        LEFT JOIN decoded_event_logs d USING (block_number, tx_hash, event_index)
        LEFT JOIN {{ ref('scores__event_sigs') }} e USING (event_sig)
    ),
    all_transfers AS (
        SELECT
            block_date,
            origin_from_address,
            origin_to_address,
            contract_address,
            event_index,
            block_timestamp,
            block_number,
            tx_hash,
            CASE
                WHEN topics[0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND data = '0x'
                AND topics[3] IS NOT NULL THEN 'erc721_transfer'
                WHEN topics[0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN 'erc1155_transfer'
                WHEN topics[0] :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb' THEN 'erc1155_transfer_batch'
                WHEN topics[0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND SUBSTR(data, 3, 64) IS NOT NULL
                AND SUBSTR(topics[1], 27, 40) IS NOT NULL
                AND SUBSTR(topics[2], 27, 40) IS NOT NULL THEN 'erc20_transfer'
                ELSE NULL
            END AS transfer_type,
            IFF(
                transfer_type = 'erc20_transfer',
                TRY_TO_NUMBER(utils.udf_hex_to_int(SUBSTR(data, 3, 64))) / POW(10, COALESCE(c.decimals, 18)),
                NULL
            ) AS value,
            CASE
                WHEN transfer_type IN ('erc20_transfer', 'erc721_transfer') THEN CONCAT('0x', SUBSTR(topics[1], 27, 40)) :: STRING
                WHEN transfer_type IN ('erc1155_transfer', 'erc1155_transfer_batch') THEN CONCAT('0x', SUBSTR(topics[2] :: STRING, 27, 40))
                ELSE NULL
            END AS token_from_address,
            CASE
                WHEN transfer_type IN ('erc20_transfer', 'erc721_transfer') THEN CONCAT('0x', SUBSTR(topics[2], 27, 40)) :: STRING
                WHEN transfer_type IN ('erc1155_transfer', 'erc1155_transfer_batch') THEN CONCAT('0x', SUBSTR(topics[3] :: STRING, 27, 40))
                ELSE NULL
            END AS token_to_address,
            token_from_address = '0x0000000000000000000000000000000000000000' AS is_mint
        FROM
            event_names e
        LEFT JOIN {{ ref('core_evm__dim_contracts') }} c ON e.contract_address = c.address
        WHERE
            transfer_type IS NOT NULL
        UNION ALL
        SELECT
            block_timestamp :: DATE AS block_date,
            txs.from_address AS origin_from_address,
            txs.to_address AS origin_to_address,
            NULL AS contract_address,
            trace_index AS event_index,
            block_timestamp,
            block_number,
            tx_hash,
            'native_transfer' AS transfer_type,
            value,
            from_address AS token_from_address,
            to_address AS token_to_address,
            FALSE AS is_mint
        FROM
            native_transfers
        LEFT JOIN txs USING (block_number, tx_hash)
    ),
    labeled_transfers AS (
        SELECT
            t.*,
            l.label_type,
            CASE
                WHEN is_mint AND transfer_type = 'erc721_transfer' THEN 'n_nft_mint'
                WHEN is_mint AND transfer_type = 'erc1155_transfer' THEN 'n_nft_mint'
                WHEN is_mint AND transfer_type = 'erc1155_transfer_batch' THEN 'n_nft_mint'
                WHEN l.label_type = 'bridge' and l.label_subtype <> 'token_contract' THEN 'n_bridge_in'
                WHEN l.label_type = 'cex' THEN 'n_cex_withdrawals'
                ELSE NULL
            END AS label_metric_name,
            metric_rank
        FROM
            all_transfers t
        LEFT JOIN {{ ref('core_evm__dim_labels') }} l ON t.token_from_address = l.address
        LEFT JOIN {{ ref('scores__scoring_activity_categories') }} a ON a.metric = label_metric_name
    ),
    eligible_events AS (
        SELECT
            block_date,
            origin_from_address,
            origin_to_address,
            contract_address,
            e.event_sig,
            e.event_name,
            event_index,
            block_timestamp,
            tx_hash,
            block_number,
            l.label_type,
            s.metric AS sig_metric_name,
            n.metric AS name_metric_name,
            CASE
                WHEN l.label_type = 'bridge' THEN 'n_bridge_in'
                WHEN l.label_type = 'cex' THEN 'n_cex_withdrawals'
                WHEN l.label_type = 'dex' THEN 'n_swap_tx'
                WHEN l.label_type = 'defi' THEN 'n_other_defi'
                ELSE NULL
            END AS label_metric_name,
            COALESCE(sig_metric_name, label_metric_name, name_metric_name) AS metric_name_0,
            IFF(
                wrapped_asset_address IS NOT NULL
                AND e.event_sig = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c',
                'n_swap_tx',
                metric_name_0
            ) AS metric_name,
            metric_rank
        FROM
            event_names e
        LEFT JOIN {{ ref('core_evm__dim_labels') }} l ON contract_address = l.address
        LEFT JOIN {{ ref('scores__known_event_sigs') }} s ON s.event_sig = e.event_sig
        LEFT JOIN {{ ref('scores__known_event_names') }} n ON e.event_name ILIKE '%' || n.event_name || '%'
        LEFT JOIN {{ ref('scores__wrapped_assets') }} w ON e.contract_address = w.wrapped_asset_address AND w.blockchain = '{{ blockchain }}'
        LEFT JOIN {{ ref('scores__scoring_activity_categories') }} a ON a.metric = metric_name
        WHERE
            e.event_sig NOT IN (
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', -- transfers
                '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925' -- approvals
            )
    ),
    prioritized_eligible_events AS (
        SELECT
            block_date,
            origin_from_address,
            origin_to_address,
            contract_address,
            event_sig,
            event_name,
            event_index,
            block_timestamp,
            tx_hash,
            block_number,
            label_type,
            sig_metric_name,
            name_metric_name,
            label_metric_name,
            metric_name_0,
            metric_name,
            metric_rank
        FROM
            eligible_events
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash, event_index ORDER BY metric_rank ASC) = 1
    ), 
    create_action_details AS (
        SELECT
            block_date,
            origin_from_address,
            origin_to_address,
            event_index AS index,
            block_timestamp,
            block_number,
            tx_hash,
            transfer_type AS action_type,
            OBJECT_CONSTRUCT(
                'value', value,
                'token_from_address', token_from_address,
                'token_to_address', token_to_address,
                'label_type', label_type,
                'contract_address', contract_address
            ) AS action_details,
            label_metric_name AS metric_name,
            metric_rank
        FROM
            labeled_transfers
        UNION ALL
        SELECT
            block_date,
            origin_from_address,
            origin_to_address,
            event_index AS index,
            block_timestamp,
            block_number,
            tx_hash,
            'contract_interaction' AS action_type,
            OBJECT_CONSTRUCT(
                'event_name', event_name,
                'event_sig', event_sig,
                'label_type', label_type,
                'contract_address', contract_address
            ) AS action_details,
            metric_name,
            metric_rank
        FROM
            prioritized_eligible_events
        UNION ALL
        SELECT
            block_date,
            from_address AS origin_from_address,
            to_address AS origin_to_address,
            -1 AS index,
            block_timestamp,
            block_number,
            tx_hash,
            'tx' AS action_type,
            OBJECT_CONSTRUCT(
                'complex_tx', complex_tx,
                'to_address_is_contract', to_address_is_contract,
                'label_type', NULL
            ) AS action_details,
            NULL AS metric_name,
            NULL AS metric_rank
        FROM
            txs
    )
    SELECT
        block_date,
        origin_from_address,
        origin_to_address,
        index,
        block_timestamp,
        block_number,
        tx_hash,
        action_type,
        action_details,
        metric_name,
        metric_rank,
        '{{ blockchain }}' AS blockchain,
        {{ dbt_utils.generate_surrogate_key(['tx_hash', 'index', 'action_type', "'" ~ blockchain ~ "'"]) }} AS actions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        create_action_details
    {% else %}
        -- Return an empty result set with the correct schema
        SELECT
            CAST(NULL AS DATE) AS block_date,
            CAST(NULL AS STRING) AS origin_from_address,
            CAST(NULL AS STRING) AS origin_to_address,
            CAST(NULL AS INTEGER) AS index,
            CAST(NULL AS TIMESTAMP) AS block_timestamp,
            CAST(NULL AS INTEGER) AS block_number,
            CAST(NULL AS STRING) AS tx_hash,
            CAST(NULL AS STRING) AS action_type,
            CAST(NULL AS OBJECT) AS action_details,
            CAST(NULL AS STRING) AS metric_name,
            CAST(NULL AS INTEGER) AS metric_rank,
            CAST(NULL AS STRING) AS blockchain,
            CAST(NULL AS STRING) AS actions_id,
            CAST(NULL AS TIMESTAMP) AS inserted_timestamp,
            CAST(NULL AS TIMESTAMP) AS modified_timestamp,
            CAST(NULL AS STRING) AS _invocation_id
        WHERE 1 = 0
    {% endif %}