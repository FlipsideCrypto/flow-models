{% docs __overview__ %}

# Flipside Crypto FLOW Models Documentation

Welcome to the Flipside Crypto FLOW models! This documentation provides a comprehensive overview of the data models, tables, and views available for the Flow blockchain and related ecosystems, as curated and maintained by Flipside Crypto. These models are designed to support deep analytics, protocol research, and data-driven insights for developers, analysts, and the broader Flow community. The models follow a layered architecture (bronze, silver, gold) to ensure data quality, traceability, and performance, and are built using dbt best practices for modularity and transparency.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Beta Tables
**Dimension Tables:**
- [beta__dim_nfl_athletes](#!/model/model.flow_models.beta__dim_nfl_athletes)
- [beta__dim_nfl_teams](#!/model/model.flow_models.beta__dim_nfl_teams)
- [beta__dim_nflad_playoff_rosters](#!/model/model.flow_models.beta__dim_nflad_playoff_rosters)
**Easy Views:**
- [beta__ez_moment_player_ids](#!/model/model.flow_models.beta__ez_moment_player_ids)

### Core Tables
**Dimension Tables:**
- [core__dim_address_mapping](#!/model/model.flow_models.core__dim_address_mapping)
- [core__dim_contract_labels](#!/model/model.flow_models.core__dim_contract_labels)
- [core__dim_labels](#!/model/model.flow_models.core__dim_labels)
**Fact Tables:**
- [core__fact_blocks](#!/model/model.flow_models.core__fact_blocks)
- [core__fact_events](#!/model/model.flow_models.core__fact_events)
- [core__fact_transactions](#!/model/model.flow_models.core__fact_transactions)
**Easy Views:**
- [core__ez_token_transfers](#!/model/model.flow_models.core__ez_token_transfers)
- [core__ez_transaction_actors](#!/model/model.flow_models.core__ez_transaction_actors)

### DeFi Tables
**Dimension Tables:**
- [defi__dim_swap_pool_labels](#!/model/model.flow_models.defi__dim_swap_pool_labels)
**Fact Tables:**
- [defi__fact_bridge_activity](#!/model/model.flow_models.defi__fact_bridge_activity)
- [defi__fact_dex_swaps](#!/model/model.flow_models.defi__fact_dex_swaps)
**Easy Views:**
- [defi__ez_bridge_activity](#!/model/model.flow_models.defi__ez_bridge_activity)
- [defi__ez_dex_swaps](#!/model/model.flow_models.defi__ez_dex_swaps)

### NFT Tables
**Dimension Tables:**
- [nft__dim_allday_metadata](#!/model/model.flow_models.nft__dim_allday_metadata)
- [nft__dim_moment_metadata](#!/model/model.flow_models.nft__dim_moment_metadata)
- [nft__dim_topshot_metadata](#!/model/model.flow_models.nft__dim_topshot_metadata)
- [nft__dim_ufc_strike_metadata](#!/model/model.flow_models.nft__dim_ufc_strike_metadata)
**Fact Tables:**
- [nft__fact_topshot_buybacks](#!/model/model.flow_models.nft__fact_topshot_buybacks)
**Easy Views:**
- [nft__ez_nft_sales](#!/model/model.flow_models.nft__ez_nft_sales)

### Price Tables
**Dimension Tables:**
- [price__dim_asset_metadata](#!/model/model.flow_models.price__dim_asset_metadata)
**Fact Tables:**
- [price__fact_prices_ohlc_hourly](#!/model/model.flow_models.price__fact_prices_ohlc_hourly)
**Easy Views:**
- [price__ez_asset_metadata](#!/model/model.flow_models.price__ez_asset_metadata)
- [price__ez_prices_hourly](#!/model/model.flow_models.price__ez_prices_hourly)

### Governance Tables
**Dimension Tables:**
- [gov__dim_validator_labels](#!/model/model.flow_models.gov__dim_validator_labels)
**Easy Views:**
- [gov__ez_staking_actions](#!/model/model.flow_models.gov__ez_staking_actions)

### Rewards Tables
**Dimension Tables:**
- [rewards__dim_storefront_items](#!/model/model.flow_models.rewards__dim_storefront_items)
**Fact Tables:**
- [rewards__fact_points_balances](#!/model/model.flow_models.rewards__fact_points_balances)
- [rewards__fact_points_transfers](#!/model/model.flow_models.rewards__fact_points_transfers)
- [rewards__fact_transaction_entries](#!/model/model.flow_models.rewards__fact_transaction_entries)

### Stats Tables
**Easy Views:**
- [stats__ez_core_metrics_hourly](#!/model/model.flow_models.stats__ez_core_metrics_hourly)

### EVM Tables
**Dimension Tables:**
- [core_evm__dim_contracts](#!/model/model.flow_models.core_evm__dim_contracts)
- [core_evm__dim_labels](#!/model/model.flow_models.core_evm__dim_labels)
- [core_evm__dim_contract_abis](#!/model/model.flow_models.core_evm__dim_contract_abis)
**Fact Tables:**
- [core_evm__fact_blocks](#!/model/model.flow_models.core_evm__fact_blocks)
- [core_evm__fact_event_logs](#!/model/model.flow_models.core_evm__fact_event_logs)
- [core_evm__fact_traces](#!/model/model.flow_models.core_evm__fact_traces)
- [core_evm__fact_transactions](#!/model/model.flow_models.core_evm__fact_transactions)
**Easy Views:**
- [core_evm__ez_token_transfers](#!/model/model.flow_models.core_evm__ez_token_transfers)
- [core_evm__ez_native_transfers](#!/model/model.flow_models.core_evm__ez_native_transfers)
- [core_evm__ez_decoded_event_logs](#!/model/model.flow_models.core_evm__ez_decoded_event_logs)

---

## Data Model Overview

The FLOW models are organized in a layered architecture:
- **Bronze**: Raw data ingested from the blockchain or external sources, minimally processed.
- **Silver**: Cleaned, parsed, and enriched data, with deduplication and normalization applied.
- **Gold**: Final, analytics-ready tables and views, including core facts, dimensions, and curated "ez" views for common use cases.

Dimension tables (`dim_`) provide reference data and entity mappings. Fact tables (`fact_`) record core blockchain events and transactions. Easy views (`ez_`) combine facts and dimensions for simplified analytics.

<llm>
<blockchain>Flow</blockchain>
<aliases>FLOW</aliases>
<ecosystem>Layer 1, Cadence</ecosystem>
<description>Flow is a high-throughput, developer-friendly Layer 1 blockchain designed for consumer applications, NFTs, and gaming. It uses a unique multi-role architecture and Cadence smart contract language to optimize for scalability and usability. Flow powers major NFT projects (NBA Top Shot, NFL All Day) and supports both native and EVM-compatible assets via bridges. Its architecture enables fast, low-cost transactions and a rich ecosystem for developers and users.</description>
<external_resources>
    <block_scanner>https://flowscan.org/</block_scanner>
    <developer_documentation>https://developers.flow.com/</developer_documentation>
</external_resources>
<expert>
  <constraints>
    <table_availability>
      Use only tables and schemas listed in the Quick Links section for Flow analytics.
    </table_availability>
    <schema_structure>
      Gold models are organized by schema (core, defi, nft, etc.) and type (dim_, fact_, ez_). EVM models are in the core_evm schema.
    </schema_structure>
  </constraints>
  <optimization>
    <performance_filters>
      Filter by block_timestamp or date fields for efficient queries. Use indexed columns for large tables.
    </performance_filters>
    <query_structure>
      Prefer CTEs for readability. Avoid deeply nested subqueries. Use incremental logic for large models.
    </query_structure>
    <implementation_guidance>
      Use window functions and aggregations judiciously. Leverage clustering and partitioning for performance.
    </implementation_guidance>
  </optimization>
  <domain_mapping>
    <token_operations>
      Use core__ez_token_transfers for Flow native and bridged token transfers. For EVM, use core_evm__ez_token_transfers.
    </token_operations>
    <defi_analysis>
      Use defi__ez_bridge_activity and defi__ez_dex_swaps for DeFi analytics. EVM DeFi is not natively supported on Flow.
    </defi_analysis>
    <nft_analysis>
      Use nft__ez_nft_sales and nft__dim_allday_metadata for NFT analytics. Flow is a leading NFT chain.
    </nft_analysis>
    <specialized_features>
      Flow's Cadence language and multi-role architecture are unique. EVM compatibility is via bridge only.
    </specialized_features>
  </domain_mapping>
  <interaction_modes>
    <direct_user>
      Ask clarifying questions for complex analytics or cross-chain queries.
    </direct_user>
    <agent_invocation>
      When invoked by another agent, respond with relevant SQL or model references.
    </agent_invocation>
  </interaction_modes>
  <engagement>
    <exploration_tone>
      Have fun exploring the Flow ecosystem and its rich data landscape!
    </exploration_tone>
  </engagement>
</expert>
</llm>

{% enddocs %}
