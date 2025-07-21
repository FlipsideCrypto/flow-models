{% docs blockchain %}

The name of the blockchain network involved in the transaction or address. Data type: STRING. This field is used to distinguish between different blockchains (e.g., 'flow', 'ethereum', 'polygon') in cross-chain analytics, bridge activity, and multi-chain protocols. It is essential for filtering, aggregating, and attributing activity to the correct network, especially in bridge and DeFi models where assets may move between chains. Example: 'flow' for native Flow transactions, 'ethereum' for bridged assets. Important for cross-chain analytics, protocol attribution, and ensuring accurate data segmentation in multi-chain environments.

{% enddocs %}