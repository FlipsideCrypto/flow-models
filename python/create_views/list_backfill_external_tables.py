import csv
import os

csv_file_path = '../../data/seeds__network_version.csv'

# Read the network data from the CSV
with open(csv_file_path, 'r') as f:
    reader = csv.DictReader(f)
    network_version = [row['network_version'].replace('-', '_').upper() for row in reader]

methods = ["BLOCKS", "COLLECTIONS", "TRANSACTIONS", "TRANSACTION_RESULTS"]

# list external table names
table_names = []
for method in methods:
    for network in network_version:
        table_names.append(f"{method}_{network}")

# Write output to yml file, copy to sources.yml
with open('network_versions.yml', 'w') as f:
    for table_name in table_names:
        f.write(f"- name: {table_name}\n")

# output table names for copying to sql file
with open("table_names.md", "w") as f:
    f.write(f"{table_names}\n")
