# Flow EVM Models

Separating out Flow EVM models from flow core, for now. TBD


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
