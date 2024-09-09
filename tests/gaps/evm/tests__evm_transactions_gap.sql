{{ config(
    tags = ['evm_gap_test']
) }}

WITH tx_per_block AS (

    SELECT
        block_hash,
        block_number,
        COUNT(
            DISTINCT tx_hash
        ) AS tx_count
    FROM
        {{ ref('silver_evm__transactions') }}

        {% if not var(
                'DBT_TEST_FULL',
                False
            ) %}
        WHERE
            _inserted_timestamp >= SYSDATE() - INTERVAL '7 days'
        {% endif %}
    GROUP BY
        1,
        2
),
blocks AS (
    SELECT
        block_hash,
        block_number,
        transaction_count AS tx_count,
        _inserted_timestamp
    FROM
        {{ ref('silver_evm__blocks') }}
    WHERE
        transaction_count > 0 
        
        {% if not var(
                'DBT_TEST_FULL',
                False
            ) %}
            AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 days'
        {% endif %}
)
SELECT
    b.block_number,
    b.block_hash,
    b.tx_count AS block_tx_count,
    t.tx_count AS tx_count,
    b._inserted_timestamp
FROM
    blocks b
    LEFT JOIN tx_per_block t
    ON b.block_number = t.block_number
WHERE
    b.tx_count != t.tx_count 
    -- may be some temporarily missing blocks at chainhead, only issue if not filled on subsequent run
    AND _inserted_timestamp <= SYSDATE() - INTERVAL '1 hour'
