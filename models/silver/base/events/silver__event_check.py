import pandas as pd
import requests as res


def model(dbt, session):

    # table is the default materialization
    # https://docs.getdbt.com/docs/build/python-models#materializations
    dbt.config(materialized="table", packages=["pandas", "requests"])

    event_diff = dbt.ref("silver__event_diff_sample")

    # tx_sample = event_diff.select("tx_id").limit(1)

    tx_sample = "000010344f0dd5995d7d443fed18d62a65b297b65217b1790bebde1b1bcdbb00"

    url = f"https://rest-mainnet.onflow.org/v1/transaction_results/{tx_sample}"
    r = res.get(url).json()

    final_df = pd.json_normalize(r)

    return final_df
