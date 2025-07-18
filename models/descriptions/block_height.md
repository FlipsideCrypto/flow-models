
{% docs block_height %}
Block height is the unique, sequential integer assigned to each block as it is added to the Flow blockchain. It serves as the primary identifier for block ordering and is used to reference the position of a block within the chain. Data type: INTEGER. Block height is essential for joining block, transaction, and event tables, and for analyzing blockchain growth over time. For example, block height 100,000 refers to the 100,000th block produced on Flow. This field is critical for time-series analytics, chain reorganization analysis, and historical state reconstruction.
{% enddocs %}
