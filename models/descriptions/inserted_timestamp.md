{% docs inserted_timestamp %}
The UTC timestamp when the record was first created and inserted into this table. Data type: TIMESTAMP_NTZ. Used for ETL auditing, tracking data freshness, and identifying when data was loaded or updated in the analytics pipeline. Example: '2023-01-01 12:00:00'. This field is critical for monitoring data latency, troubleshooting ETL issues, and supporting recency tests in dbt.
{% enddocs %}
