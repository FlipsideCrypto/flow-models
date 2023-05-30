{% macro create_flow_txs_api() %}
  {% set create_table %}
  CREATE schema if NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.flow_api_check(
    tx_id VARCHAR,
    block_height NUMBER,
    res VARIANT,
    _request_timestamp TIMESTAMP_NTZ,
    has_events BOOLEAN
  );
{% endset %}
  {% do run_query(create_table) %}

{% set livequery %}
{% set step = 1 %}


CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.flow_txs_api() 
RETURNS OBJECT
LANGUAGE SQL 
EXECUTE AS CALLER
AS $$
DECLARE
  counter NUMBER DEFAULT 0;
  UPDATE_FLAG_SUCCESS VARCHAR DEFAULT ('update flow_dev.silver.empty_event_txs
                  set is_confirmed = True 
                  where tx_id in (select tx_id from response_data)');
  UPDATE_FLAG_ERROR VARCHAR DEFAULT ('update flow_dev.silver.empty_event_txs
                  set is_api_error = TRUE 
                  where tx_id in (select tx_id from flow_dev.silver.empty_event_txs
                                  where not not_is_confirmed and not is_api_error
                                  order by _inserted_timestamp desc)');
  START_TIME TIMESTAMP_NTZ DEFAULT SYSDATE();

BEGIN
REPEAT
CREATE
  OR REPLACE TRANSIENT TABLE response_data AS WITH 
  sample_txs as (
      select 
        tx_id, 
        block_height,
        _inserted_timestamp
      from {{ target.database }}.silver.empty_event_txs
        where not is_confirmed and not is_api_error
          and tx_id in 
          (
          '8714ffcceaed1ac4ecbbaffc235479a763173250371e110487a224a909409d37',
          'fb1f30c48fc8a0cb5e04dc6a201fdf84187ab8cbfefea9efebbbf120f793302f',
          '9a0aaa02902cf71da5f62d5a19be2fc65e1987a4c265da8167ef56deac948d18'
          )
        order by _inserted_timestamp desc
        limit 1
  ),
  call as (
      SELECT
          tx_id,
          block_height,
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
    res,
    _request_timestamp,
    array_size(res:data:events::array) > 0 as has_events
from call;

INSERT INTO {{ target.database }}.bronze_api.flow_api_check
SELECT * FROM response_data;

EXECUTE IMMEDIATE :UPDATE_FLAG_SUCCESS;

counter:= counter + 1;


UNTIL (
    counter = 3
  )
END REPEAT;
RETURN OBJECT_CONSTRUCT('Calls', :counter,
                        'Duration', datediff(second, :START_TIME, SYSDATE()),
                        'Txs_retrieved', (select array_agg(tx_id) from {{ target.database}}.bronze_api.flow_api_check where _request_timestamp >= :START_TIME)
                        );

EXCEPTION
  WHEN statement_error THEN
    IF (sqlcode = 100353) THEN
      EXECUTE IMMEDIATE :UPDATE_FLAG_ERROR;
    END IF;

    RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);

END;$$ 

{% endset %}
{% do run_query(livequery) %}




{% endmacro %}


