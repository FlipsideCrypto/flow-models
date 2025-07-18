{% docs modified_timestamp %}
The UTC timestamp when this record was last updated or modified by an internal ETL or dbt process. Data type: TIMESTAMP_NTZ. Used for change tracking, ETL auditing, and identifying the most recent update to a record. Example: '2023-01-02 15:30:00'. This field is important for troubleshooting data issues, monitoring pipeline health, and supporting recency or freshness tests in dbt.
{% enddocs %}
