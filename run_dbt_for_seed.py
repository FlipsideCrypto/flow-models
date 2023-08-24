# run_dbt_for_seed.py
import csv
import subprocess
import time


def run_dbt_for_model(model_name, node_url, root_height, end_height):
    cmd = [
        "dbt",
        "run",
        "--threads",
        "8",
        "--vars",
        f'{{"node_url":"{node_url}", "start_block":{root_height}, "end_block":{end_height},"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}}',
        "-m",
        f"streamline__get_{model_name}_history",
        "--profile",
        "flow",
        "--target",
        "prod",
    ]

    subprocess.run(cmd)


seed_file = "./data/seeds__network_version.csv"

with open(seed_file, "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        root_height = row["root_height"]
        node_url = row["node_url"]
        end_height = row["end_height"]

        run_dbt_for_model("blocks", node_url, root_height, end_height)
        # sleep for 15 minutes to allow for the blocks model to finish
        time.sleep(900)
        run_dbt_for_model("collections", node_url, root_height, end_height)
        time.sleep(900)
        run_dbt_for_model("transactions", node_url, root_height, end_height)
        time.sleep(900)
        run_dbt_for_model("transaction_results", node_url, root_height, end_height)
