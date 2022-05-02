{{ config(
  materialized = 'incremental',
  cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'block_height',
  incremental_strategy = 'delete+insert'
) }}

WITH bronze_blocks AS (

  SELECT
    *
  FROM
    {{ ref('bronze__blocks') }}

{% if is_incremental() %}
WHERE
  ingested_at :: DATE >= getdate() - INTERVAL '2 days'
{% endif %}

qualify ROW_NUMBER() over (
  PARTITION BY block_id
  ORDER BY
    ingested_at DESC
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
    COALESCE(
      header :block_header :collection_guarantee,
      header :collection_guarantee
    ) :: variant AS collection_guarantee,
    ingested_at
  FROM
    bronze_blocks
)
SELECT
  *
FROM
  silver_blocks
