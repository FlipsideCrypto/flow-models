# Flow EVM Models

Separating out Flow EVM models from flow core.

```shell
dbt run -s 2+tag:streamline_realtime_evm --vars '{"STREAMLINE_INVOKE_STREAMS": True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}'
```

## TODO
 - check dbt models tags (evm, evm_testnet, testnet, crescendo)
 - ALIGN column names both internally and externally
 - update GHA workflows
   1. deprecate flow testnet
   1. add flowEVM on hourly (?) schedule
 - gold views
 - roadmap in README (?) 

## Tooling
 - UDFs
  - https://github.com/FlipsideCrypto/flow-models/pull/337/
  - streamline.udf_bulk_rest_api_v2
  - streamline.udf_bulk_decode_logs
  - streamine.udf_get_evm_chainhead(STRING optional) (new!)
  - streamline.udf_udf_decode_hash_array(ARRAY) (new!)

## External Resources

 - https://developers.flow.com/evm/networks
   - https://developers.flow.com/evm/using#json-rpc-methods
  - RPC Endpoints:
    - https://previewnet.evm.nodes.onflow.org
    - https://testnet.evm.nodes.onflow.org
 - https://ethereum.org/en/developers/docs/apis/json-rpc/
 - https://developers.flow.com/networks/flow-networks/accessing-testnet
 - https://github.com/onflow/flow-evm-gateway
    - https://flowfoundation.notion.site/EVM-Gateway-Deployment-3c41da6710af40acbaf971e22ce0a9fd
 - https://evm-testnet.flowscan.io/
 - https://contractbrowser.com/

## RPC Methods
 1. [eth_blockNumber](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_blocknumber) for chainhead
 1. [eth_getBlockByNumber](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getblockbynumber) with param 2 = TRUE for blocks and txs
 1. [eth_getBlockReceipts](https://www.quicknode.com/docs/ethereum/eth_getBlockReceipts)
 1. [eth_getLogs](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getlogs) --- maybe not needed
 1. [debug_traceBlockByNumber](https://www.quicknode.com/docs/ethereum/debug_traceBlockByNumber)
 1. [eth_getTransactionCount](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_gettransactioncount) --- maybe not needed
  - maybe for get receipt and trace pipelines


 - Note, for methods that accept a block height, that height must be hex encoded using:
 ```sql
CONCAT('0x', TRIM(to_char(block_height, 'XXXXXXXX')))
```

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
            'https://testnet.evm.nodes.onflow.org',
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

## Schema Design
### Transactions
Can query transactions within blocks for which we already have data:
```sql
select * from streamline.flow_dev.evm_testnet_blocks_stg
where data:error:code is null
and array_size(data:result:transactions) > 0
limit 5;
```

## New Utils
### UDFs

### Decode Transaction Hash
An EVM Executed Transaction is encoded in the event data when the method `TransactionExecuted` is executed on the contract `A.8c5303eaa26202d6.EVM` (note - testnet address). The EVM block height is logged in `event_data:blockHeight` and the EVM transaction hash is encoded as a byte array. SQL can be used to decode, or more simply:

```python
CREATE OR REPLACE FUNCTION decode_hash_array(raw_array ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'decode_hash_array'
AS
$$
def decode_hash_array(raw_array):
    try:
        # Parse the JSON array
        data = raw_array
        
        # Extract and convert values
        hex_values = [format(int(item['value']), '02x') for item in data]
        
        # Concatenate and add prefix
        result = '0x' + ''.join(hex_values)
        
        return result.lower()
    except Exception as e:
        return f"Error: {str(e)}"
$$
;
```

### Get EVM Chainhead
EVM Chainhead can be retried with a simple LiveQuery call to eth_getBlock.

```sql
    select
    livequery.utils.udf_hex_to_int(
        flow.live.udf_api(
            'POST',
            'https://testnet.evm.nodes.onflow.org',
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