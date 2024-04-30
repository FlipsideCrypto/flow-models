# Log ESPN Player IDs
Python script to query data from the ESPN API and store it in Snowflake for prod use. Presently, the player endpoint is what we will likely need to run again before the season starts.  

The teams endpoint can be set by uncommenting lines 115-122 and commenting out 124-132. The target table is presently set in-line in the script.  


## .env
Script will load the following config variables to create a Snowflake connection. Write access to prod db is required, as the data ingested from the ESPN API is uploaded to a bronze table.
```
USERNAME=<name>@flipsidecrypto.com
ROLE=<PROD_ROLE>
ACCOUNT=<SF_ACCT>
WAREHOUSE=<>
DATABASE=<>
SCHEMA=<>
```
