{% docs delegator %}

The Flow account address for the wallet delegating stake to a validator node. Data type: STRING. This field represents the address of the account that is delegating FLOW tokens to a validator node for staking operations. Used for delegator identification, staking analysis, and governance operations. Example: '0x1234567890abcdef' for a specific delegator account address. Note: This is derived from the first authorizer on the transaction and may be inaccurate if the transaction was signed by an EOA. Refer to delegator_id as the unique identifier for the delegator, taken directly from the emitted event data.

{% enddocs %}
