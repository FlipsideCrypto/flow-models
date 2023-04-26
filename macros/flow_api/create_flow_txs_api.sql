{% macro create_flow_txs_api() %}
  {% set create_table %}
  CREATE schema if NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.flow_api_check(
    tx_id VARCHAR,
    block_height NUMBER,
    sample_index NUMBER,
    res VARIANT,
    _request_timestamp TIMESTAMP_NTZ
  );
{% endset %}
  {% do run_query(create_table) %}

{% set livequery %}
{% set step = 1 %}

CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.flow_txs_api() 
RETURNS BOOLEAN 
LANGUAGE SQL 
EXECUTE AS CALLER
AS $$
BEGIN
  let counter:= 0;
  let max_index := 1416360;
REPEAT
CREATE
  OR REPLACE temporary TABLE response_data AS WITH 
sample_txs as (
    select distinct tx_id, block_height, sample_index from flow_dev.silver.event_diff_2
    where sample_index > :max_index 
      and sample_index <= (:max_index + {{ step }})
      and sample_index not in (1268528, 1416288, 1416317, 1416335, 1416338, 1416361, 1416363)
),
call as (
    SELECT
        tx_id,
        block_height,
        sample_index,
        concat('https://rest-mainnet.onflow.org/v1/transaction_results/', tx_id) as url,
        udfs.streamline.udf_api(
            'GET',
            url,
            {},
            {}
        ) AS res,
      CURRENT_TIMESTAMP AS _request_timestamp
    from sample_txs
)
select
    tx_id,
    block_height,
    sample_index,
    res,
    _request_timestamp
from call;
INSERT INTO {{ target.database }}.bronze_api.flow_api_check
SELECT * FROM response_data;

max_index:= max_index + {{ step }};
counter:= counter + 1;


UNTIL (
    max_index >= 1416515
  )
END REPEAT;
RETURN TRUE;
END;$$ 

{% endset %}
{% do run_query(livequery) %}

{% endmacro %}


