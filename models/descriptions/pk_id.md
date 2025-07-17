{% docs pk_id %}
`pk_id` is a surrogate primary key, uniquely generated for each row in the table. Data type: STRING or INTEGER (implementation-specific). This field ensures every record is uniquely identifiable, even if the source data lacks a natural primary key. Used for efficient joins, deduplication, and as a reference in downstream models. Example: an auto-incremented integer or a UUID string. Essential for maintaining data integrity and supporting dbt tests for uniqueness.
{% enddocs %}
