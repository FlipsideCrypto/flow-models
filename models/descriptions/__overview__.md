{% docs __overview__ %}

# Welcome to the Flipside Crypto FLOW Models Documentation

## **What does this documentation cover?**

The documentation included here details the design of the FLOW
tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/flow-models/)

## **How do I use these docs?**

The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Beta Tables (`FLOW`.`BETA`.`<table_name>`)

- [ez_nft_topshot_packs](#!/model/model.flow_models.beta__ez_nft_topshot_packs)

### Core Tables (`flow`.`CORE`.`<table_name>`)

- [dim_contract_labels](#!/model/model.flow_models.core__dim_contract_labels)
- [ez_token_transfers](#!/model/model.flow_models.core__ez_token_transfers)
- [fact_blocks](#!/model/model.flow_models.core__fact_blocks)
- [fact_events](#!/model/model.flow_models.core__fact_events)
- [fact_transactions](#!/model/model.flow_models.core__fact_transactions)

### DeFi Tables (`flow`.`DEFI`.`<table_name>`)

- [dim_swap_pool_labels](#!/model/model.flow_models.defi__dim_swap_pool_labels)
- [ez_bridge_transactions](#!/model/model.flow_models.defi__ez_bridge_transactions)
- [ez_swaps](#!/model/model.flow_models.defi__ez_swaps)

### Governance Tables (`flow`.`GOV`.`<table_name>`)

- [dim_validator_labels](#!/model/model.flow_models.gov__dim_validator_labels)
- [ez_staking_actions](#!/model/model.flow_models.gov__ez_staking_actions)

### NFT Tables (`flow`.`NFT`.`<table_name>`)

- [dim_allday_metadata](#!/model/model.flow_models.nft__dim_allday_metadata)
- [dim_moment_metadata](#!/model/model.flow_models.nft__dim_moment_metadata)
- [dim_topshot_metadata](#!/model/model.flow_models.nft__dim_topshot_metadata)
- [ez_nft_sales](#!/model/model.flow_models.nft__ez_nft_sales)

### Price Tables (`flow`.`PRICE`.`<table_name>`)

- [fact_hourly_prices](#!/model/model.flow_models.price__fact_hourly_prices)
- [fact_prices](#!/model/model.flow_models.price__fact_prices)

### Stats Tables (flow.stats)

- [ez_core_metrics_hourly](https://flipsidecrypto.github.io/flow-models/#!/model/model.flow_models.stats__ez_core_metrics_hourly)

## **Data Model Overview**

The FLOW
models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez\_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**

### Navigation

You can use the `Project` and `Database` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are _not_ shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the `--models` and `--exclude` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.

### **More information**

- [Flipside](https://flipsidecrypto.xyz/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/flow-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}
