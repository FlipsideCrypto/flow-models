# run_dbt_for_seed.py
import csv
import subprocess
import sys
import time


def run_dbt_for_model(model_name, node_url, root_height, end_height, use_dev=False):
    cmd = [
        "dbt",
        "run",
        "--threads",
        "8",
        "--vars",
        f'{{"node_url":"{node_url}", "start_block":{root_height}, "end_block":{end_height},"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":{use_dev}}}',
        "-m",
        f"streamline__get_{model_name}_history"
    ]

    subprocess.run(cmd)

def main(model_name, use_dev=False):
    seed_file = "./data/seeds__network_version.csv"

    with open(seed_file, "r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            root_height = row["root_height"]
            node_url = row["node_url"]
            end_height = row["end_height"]

            run_dbt_for_model(model_name, node_url, root_height, end_height, use_dev)

if __name__ == "__main__":
    # accept model name as cli argument and pass to main
    model_name = sys.argv[1]
    # acceptable model names: blocks, collections, transactions, transaction_results
    if model_name not in ["blocks", "collections", "transactions", "transaction_results"]:
        raise ValueError("model_name must be one of the following: blocks, collections, transactions, transaction_results")

    # use_dev is optional cli argument, set only if exists
    use_dev = sys.argv[2] if len(sys.argv) > 2 else False

    # use_dev can only accept True or False, raise error if not
    if use_dev not in ["True", "False"]:
        raise ValueError("use_dev can only accept True or False")

    main(model_name, use_dev)
