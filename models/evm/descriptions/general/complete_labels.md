{% docs evm_labels_table %}

This table contains labels for addresses on this EVM blockchain. 

{% enddocs %}


{% docs evm_table_dim_labels %}

The labels table is a store of one-to-one address identifiers, or an address name. Labels are broken out into a "type" (such as cex, dex, dapp, games, etc.) and a "subtype" (ex: contract_deployer, hot_wallet, token_contract, etc.) in order to help classify each address name into similar groups. Our labels are sourced from many different places, but can primarily be grouped into two categories: automatic and manual. Automatic labels are continuously labeled based on certain criteria, such as a known contract deploying another contract, behavior-based algorithms for finding deposit wallets, and consistent data pulls of custom protocol APIs. Manual labels are done periodically to find addresses that cannot be found programmatically such as finding new protocol addresses, centralized exchange hot wallets, or trending addresses. Labels can also be added by our community by using our add-a-label tool (https://science.flipsidecrypto.xyz/add-a-label/) or on-chain with near (https://near.social/lord1.near/widget/Form) and are reviewed by our labels team. A label can be removed by our labels team if it is found to be incorrect or no longer relevant; this generally will only happen for mislabeled deposit wallets.

{% enddocs %}

{% docs evm_project_name %}

The name of the project for this address. 

{% enddocs %}


{% docs evm_label %}

The label for this address. 

{% enddocs %}


{% docs evm_label_address %}

Address that the label is for. This is the field that should be used to join other tables with labels. 

{% enddocs %}


{% docs evm_label_address_name %}

The most granular label for this address.  

{% enddocs %}


{% docs evm_label_blockchain %}

The name of the blockchain.

{% enddocs %}


{% docs evm_label_creator %}

The name of the creator of the label.

{% enddocs %}


{% docs evm_label_subtype %}

A sub-category nested within label type providing further detail.

{% enddocs %}


{% docs evm_label_type %}

A high-level category describing the address's main function or ownership.

{% enddocs %}


