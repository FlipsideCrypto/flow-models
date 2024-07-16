# Flow EVM Models

Separating out Flow EVM models from flow core, for now. TBD  
Dir named `previewnet` as testnet is not yet live, but using `testnet` naming convention to not have to update later.  

```shell
dbt run -s 2+streamline__get_evm_testnet_blocks_realtime --vars '{"STREAMLINE_INVOKE_STREAMS": True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}'
```

## TODO

 - deploy external tables to streamline-snowflake
  - https://github.com/FlipsideCrypto/streamline-snowflake/blob/AN-5203-FLOW-EVM-TESTNET/models/flow.yml
   - `select * from streamline.flow_dev.evm_testnet_blocks;`
 - deploy streamline models
  1. update source
   - requires deployment of external tables to streamline db
  1. bronze views
    - bronze__streamline_evm_testnet_blocks
    - bronze__streamline_fr_evm_testnet_blocks
  1. complete model
  1. streamline_get model
   - may require a get chainhead function

## Tooling
 - UDFs
  - https://github.com/FlipsideCrypto/flow-models/pull/337/
  - streamline.udf_bulk_rest_api_v2
  - streamline.udf_bulk_decode_logs

## External Resources

 - https://developers.flow.com/evm/networks
  - https://previewnet.evm.nodes.onflow.org
 - https://ethereum.org/en/developers/docs/apis/json-rpc/


### Querying Previewnet
```shell
curl -X POST https://previewnet.evm.nodes.onflow.org \
-H "Content-Type: application/json" \
-d '{
  "method": "eth_chainId",
  "id": 1,
  "jsonrpc": "2.0",
  "params": []
}'
```

```sql
select
    livequery.utils.udf_hex_to_int(
        flow.live.udf_api(
            'POST',
            'https://previewnet.evm.nodes.onflow.org',
            {},
            object_construct(
                'method', 'eth_blockNumber',
                'id', 1,
                'jsonrpc', '2.0',
                'params', []
            )
        ):data:result
    ) as block_number
```

## Deploying dev UDFs
```sql
use database flow_dev;
use role dbt_cloud_flow;

CREATE api integration IF NOT EXISTS aws_flow_evm_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/flow-api-stg-rolesnowflakeudfsAF733095-tPEdygwPC6IV' api_allowed_prefixes = (
    'https://pfv9lhg3kg.execute-api.us-east-1.amazonaws.com/stg/'
) enabled = TRUE;

CREATE
OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
    json OBJECT
) returns ARRAY api_integration = 
aws_flow_evm_api_dev AS 'https://pfv9lhg3kg.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api';

CREATE
OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_logs(
    json OBJECT
) returns ARRAY api_integration =
    aws_flow_evm_api_dev AS'https://pfv9lhg3kg.execute-api.us-east-1.amazonaws.com/stg/bulk_decode_logs';

grant usage on integration AWS_FLOW_EVM_API_DEV to role internal_dev;

```
