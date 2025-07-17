



{% docs prices_provider %}

The data provider or source for the asset metadata or price record. Data type: STRING. This field identifies the external service, API, or protocol that supplied the asset or price data (e.g., 'CoinGecko', 'CoinMarketCap', 'Dapper', 'Flow Oracle'). Used for data quality analysis, source attribution, and filtering by provider. Example: 'CoinGecko' for token prices, 'Dapper' for Flow-native asset metadata. Important for understanding data provenance, resolving discrepancies, and ensuring data reliability in analytics workflows.

{% enddocs %}

{% docs prices_asset_id %}

The unique identifier representing the asset within the price provider's system. Data type: STRING. This field serves as the primary key for identifying specific assets across different price providers and data sources. Used for joining price data with asset metadata, tracking price movements over time, and maintaining referential integrity between fact and dimension tables. Example: 'bitcoin' for Bitcoin on CoinGecko, 'ethereum' for Ethereum on CoinMarketCap. Critical for data lineage, asset identification, and cross-provider data reconciliation in financial analytics and reporting workflows.

{% enddocs %}

{% docs prices_name %}

The full name of the asset as provided by the price data source. Data type: STRING. This field contains the human-readable, official name of the cryptocurrency, token, or digital asset. Used for display purposes, user interfaces, reporting, and asset identification in analytics dashboards. Example: 'Bitcoin' for BTC, 'Ethereum' for ETH, 'Flow' for FLOW. Important for user experience, data presentation, and maintaining consistency across different price providers and blockchain networks.

{% enddocs %}

{% docs prices_symbol %}

The ticker symbol or abbreviated code representing the asset. Data type: STRING. This field contains the short, standardized identifier used in trading, price displays, and market data. Used for quick asset identification, trading interfaces, price feeds, and cross-platform asset matching. Example: 'BTC' for Bitcoin, 'ETH' for Ethereum, 'FLOW' for Flow, 'USDC' for USD Coin. Critical for market data integration, trading operations, and maintaining consistency across different exchanges and price providers.

{% enddocs %}

{% docs prices_token_address %}

The blockchain-specific contract address or identifier for the token asset. Data type: STRING. This field contains the unique address where the token contract is deployed on its respective blockchain network. Used for token identification, contract verification, cross-chain asset mapping, and blockchain-specific operations. Example: '0xa0b86a33e6441b8c4c8c8c8c8c8c8c8c8c8c8c8c' for USDC on Ethereum, NULL for native assets like Bitcoin or Flow. Critical for DeFi operations, smart contract interactions, and maintaining accurate token mappings across different blockchain networks.

{% enddocs %}

{% docs prices_blockchain %}

The blockchain network or platform where the asset is native or primarily traded. Data type: STRING. This field identifies the specific blockchain ecosystem that hosts the asset's smart contract or where the native asset operates. Used for cross-chain analytics, multi-chain portfolio tracking, and blockchain-specific filtering and grouping. Example: 'ethereum' for ETH and ERC-20 tokens, 'flow' for FLOW and Flow-native assets, 'bitcoin' for BTC. Critical for understanding asset distribution across blockchain networks, cross-chain bridge analysis, and multi-chain DeFi analytics.

{% enddocs %}

{% docs prices_blockchain_id %}

The unique numeric identifier for the blockchain network or platform. Data type: INTEGER. This field provides a standardized way to identify blockchain networks across different systems and APIs. Used for blockchain network identification, cross-platform data integration, and maintaining consistent blockchain references. Example: 1 for Ethereum mainnet, 137 for Polygon, 0 for Bitcoin. Important for chain ID validation, cross-chain bridge operations, and maintaining referential integrity in multi-chain analytics systems.

{% enddocs %}

{% docs prices_decimals %}

The number of decimal places used to represent the smallest unit of the asset. Data type: INTEGER. This field indicates the precision with which the asset can be divided and is essential for accurate price calculations and token amount conversions. Used for token amount normalization, price precision calculations, and ensuring accurate financial computations. Example: 18 for most ERC-20 tokens, 8 for Bitcoin, 6 for USDC. Critical for DeFi operations, token transfers, and maintaining precision in financial calculations across different blockchain networks.

{% enddocs %}

{% docs prices_is_native %}

A boolean flag indicating whether the asset is the native token of its respective blockchain network. Data type: BOOLEAN. This field distinguishes between native blockchain tokens and smart contract-based tokens. Used for filtering native vs. token assets, blockchain-specific analytics, and understanding asset distribution across different token types. Example: TRUE for Bitcoin (BTC), Ethereum (ETH), and Flow (FLOW), FALSE for USDC, DAI, and other smart contract tokens. Important for blockchain economics analysis, native token price tracking, and understanding the relationship between native tokens and their ecosystem tokens.

{% enddocs %}

{% docs prices_is_deprecated %}

A boolean flag indicating whether the asset has been deprecated or is no longer actively supported by the price provider. Data type: BOOLEAN. This field helps identify assets that may have outdated or unreliable price data. Used for data quality filtering, identifying discontinued assets, and ensuring data reliability in analytics and trading operations. Example: TRUE for deprecated tokens or assets no longer listed on exchanges, FALSE for actively traded assets. Critical for data quality management, avoiding stale price data, and maintaining accurate market analytics.

{% enddocs %}

{% docs prices_id_deprecation %}

Deprecating soon! Please use the `asset_id` column instead.

{% enddocs %}

{% docs prices_decimals_deprecation %}

Deprecating soon! Please use the decimals column in `ez_asset_metadata` or join in `dim_contracts` instead.

{% enddocs %}

{% docs prices_hour %}

The timestamp representing the hour when the price data was recorded or aggregated. Data type: TIMESTAMP. This field provides the temporal reference for hourly price data and is used for time-series analysis, price trend calculations, and temporal data aggregation. Used for hourly price tracking, time-based filtering, and chronological analysis of price movements. Example: '2024-01-15 14:00:00' for the hour starting at 2 PM on January 15, 2024. Critical for time-series analytics, price volatility calculations, and maintaining temporal consistency in financial data analysis.

{% enddocs %}

{% docs prices_price %}

The closing price of the asset for the specified hour, denominated in USD. Data type: FLOAT. This field represents the final trading price of the asset during the hour and is used for price analysis, portfolio valuation, and market performance calculations. Used for price trend analysis, asset valuation, and financial reporting. Example: 45000.50 for Bitcoin price at $45,000.50 USD per BTC. Critical for portfolio management, price impact analysis, and maintaining accurate financial records for trading and investment operations.

{% enddocs %}

{% docs prices_is_imputed %}

A boolean flag indicating whether the price data was imputed or derived from previous records rather than being directly observed. Data type: BOOLEAN. This field helps identify price data that may be less reliable due to low liquidity or inconsistent reporting from the source. Used for data quality assessment, filtering out potentially unreliable price data, and understanding data completeness. Example: TRUE for low-liquidity tokens with infrequent trading, FALSE for actively traded assets with regular price updates. Critical for data quality management, risk assessment, and ensuring accurate price analysis for trading and investment decisions.

{% enddocs %}

{% docs prices_open %}

The opening price of the asset at the beginning of the specified hour, denominated in USD. Data type: FLOAT. This field represents the first trading price of the asset during the hour and is used for OHLC (Open, High, Low, Close) price analysis and volatility calculations. Used for price range analysis, volatility measurement, and technical analysis indicators. Example: 44000.00 for Bitcoin opening at $44,000.00 USD per BTC at the start of the hour. Critical for candlestick chart analysis, price momentum calculations, and understanding intra-hour price movements.

{% enddocs %}

{% docs prices_high %}

The highest trading price of the asset during the specified hour, denominated in USD. Data type: FLOAT. This field represents the peak price reached by the asset within the hour and is used for price range analysis and volatility calculations. Used for resistance level analysis, volatility measurement, and understanding price extremes during trading periods. Example: 46000.00 for Bitcoin reaching a high of $46,000.00 USD per BTC during the hour. Critical for technical analysis, price range calculations, and understanding market sentiment and price momentum.

{% enddocs %}

{% docs prices_low %}

The lowest trading price of the asset during the specified hour, denominated in USD. Data type: FLOAT. This field represents the minimum price reached by the asset within the hour and is used for price range analysis and volatility calculations. Used for support level analysis, volatility measurement, and understanding price extremes during trading periods. Example: 43000.00 for Bitcoin reaching a low of $43,000.00 USD per BTC during the hour. Critical for technical analysis, price range calculations, and understanding market sentiment and price momentum.

{% enddocs %}

{% docs prices_close %}

The closing price of the asset at the end of the specified hour, denominated in USD. Data type: FLOAT. This field represents the final trading price of the asset during the hour and is used for OHLC (Open, High, Low, Close) price analysis and trend calculations. Used for price trend analysis, momentum calculations, and technical indicator computations. Example: 45000.50 for Bitcoin closing at $45,000.50 USD per BTC at the end of the hour. Critical for candlestick chart analysis, price trend identification, and understanding market direction and momentum.

{% enddocs %}