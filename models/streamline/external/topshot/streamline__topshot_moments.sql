{{ config (
    materialized = "incremental",
    unique_key = ["moment_id","event_contract"],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_complete_evm']
) }}

WITH mints AS (

    SELECT
        event_contract,
        event_data :momentID :: STRING AS moment_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_moments_s') }}
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'
        AND event_type = 'MomentMinted'

{% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
    (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    ),
    '1900-01-01' :: timestamp_ntz
)
{% endif %}
),
sales AS (
    SELECT
        nft_collection AS event_contract,
        nft_id AS moment_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_sales_s') }}
    WHERE
        nft_collection ILIKE '%topshot%'

{% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
    (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    ),
    '1900-01-01' :: timestamp_ntz
)
{% endif %}
),
all_topshots AS (
    SELECT
        event_contract,
        moment_id,
        _inserted_timestamp
    FROM
        mints
    UNION ALL
    SELECT
        event_contract,
        moment_id,
        _inserted_timestamp
    FROM
        sales
)
SELECT
    event_contract,
    moment_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['event_contract','moment_id']
    ) }} AS topshot_moments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_topshots qualify ROW_NUMBER() over (
        PARTITION BY event_contract,
        moment_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
