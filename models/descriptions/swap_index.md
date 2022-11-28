{% docs swap_index %}

The positioning of the swap within a transaction. The first swap will index at 0. A swap transaction may only run through 1 pool, in which case the max index will be 0. A multi-pool swap that runs through 2 pools, for example, will have 2 records with swap index 0 and 1.

{% enddocs %}