{% docs tx_index %}

Deprecating soon. Note, on previous iterations the tx index was included indicating a transactions position within a block. However, on Flow, transactions are included within a collection. A block contains a number of collections (which themselves contain the transactions). Thus, tx_index is an inaccurate view of position within a block, as it is not collection based. This column is being deprecated.

{% enddocs %}
