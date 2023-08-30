{{ config(
  materialized = 'incremental',
  cluster_by = ['_inserted_timestamp::DATE'],
  unique_key = 'block_height',
  incremental_strategy = 'delete+insert',
  tags = ['scheduled']
) }}

WITH bronze_blocks AS (

  SELECT
    *
  FROM
    {{ ref('bronze__blocks') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}

qualify ROW_NUMBER() over (
  PARTITION BY block_id
  ORDER BY
    _ingested_at DESC
) = 1
),
silver_blocks AS (
  SELECT
    block_id AS block_height,
    block_timestamp,
    network,
    chain_id,
    tx_count,
    COALESCE(
      header :block_id,
      header :block_header :block_id,
      header :id
    ) :: STRING AS id,
    COALESCE(
      header :parent_id,
      header :parentId,
      header :block_header :parent_id
    ) :: STRING AS parent_id,
    _ingested_at,
    _inserted_timestamp
  FROM
    bronze_blocks
),
network_version AS (
  SELECT
    root_height,
    network_version,
    COALESCE(LAG(root_height) over (
  ORDER BY
    network_version DESC) - 1, 'inf' :: FLOAT) AS end_height
  FROM
    {{ ref('seeds__network_version') }}
),
add_version AS (
  SELECT
    block_height,
    block_timestamp,
    network,
    v.network_version,
    chain_id,
    tx_count,
    id,
    parent_id,
    _ingested_at,
    _inserted_timestamp
  FROM
    silver_blocks b
    LEFT JOIN network_version v
    ON b.block_height BETWEEN v.root_height
    AND v.end_height
)
SELECT
  *
FROM
  add_version
