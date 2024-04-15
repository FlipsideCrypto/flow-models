{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'swaps_final_id',
    tags = ['scheduled_non_core']
) }}

WITH swaps_from_aggregator AS (

    SELECT
        block_height,
        block_timestamp,
        tx_id,
        swap_index,
        pool_address AS swap_contract,
        pool_source AS platform,
        trader,
        token_in_amount,
        token_in_contract,
        NULL AS token_in_destination,
        token_out_amount,
        token_out_contract,
        NULL AS token_out_source,
        modified_timestamp AS _modified_timestamp,
        0 AS _priority
    FROM
        {{ ref('silver__swaps_aggregator') }}

{% if is_incremental() %}
WHERE
    _modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
swaps AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        swap_index,
        swap_contract,
        NULL AS platform,
        trader,
        token_in_amount,
        token_in_contract,
        token_in_destination,
        token_out_amount,
        token_out_contract,
        token_out_source,
        modified_timestamp AS _modified_timestamp,
        1 AS _priority
    FROM
        {{ ref('silver__swaps_s') }}
    WHERE
        tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                swaps_from_aggregator
        )

{% if is_incremental() %}
AND _modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
swaps_union AS (
    SELECT
        *
    FROM
        swaps_from_aggregator
    UNION ALL
    SELECT
        *
    FROM
        swaps
) {# Note - curr prices pipeline does not include token address data, making the join difficult and
inaccurate.NEW prices models DO have this so will
ADD
    price fields WITH may RELEASE.#}
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'swap_index']
    ) }} AS swaps_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    swaps_union
