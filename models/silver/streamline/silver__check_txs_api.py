import snowflake.snowpark.functions as F
import requests as res

def model(dbt, session):

    # set config
    dbt.config(
        materialized="incremental", 
        unique_key="tx_id",
        packages=["requests"]
        )
    
    # ref upstream table with txs to check
    txs_df = dbt.ref("silver__empty_event_txs")

    test_batch = txs_df.filter(txs_df.is_confirmed == False).limit(10)
    # ref existing table with checked txs
    # todo - copy the data in this table over to here ? idk
    # todo - how do i query a non ref table / create an object of a non ref table?
    checked_txs_path = f"{ dbt.this.database }.bronze_api.flow_api_check"

    # pseudocode
    # get batch of txs from empty event txs where is_confirmed = False and is_api_error = False
    # pass batch to udf_api function
    # if all success, then write the response to a dataframe and mark is_confirmed = True in empty_event_tx
    # if error, catch exception and loop thru batch to find the tx causing the error
    # write the successful responses to a snowpark dataframe and mark is_confirmed = True in empty_event_tx
    # log tx causing error in an array
    # try requesting bad tx using base requests library to bypass the UDF 6mb limit
    # if success, write to snowpark dataframe and mark is_confirmed = True in empty_event_tx
    # if error, mark is_api_error = True in empty_event_tx
    # continue until there are no more txs where is_confirmed = False and is_api_error = False
    # finally, return the dataframe of successful txs to append to the incremental snowflake table


    return test_batch