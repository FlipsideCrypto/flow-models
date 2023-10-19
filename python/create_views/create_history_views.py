import csv
import os

# Define the paths to the CSV and SQL template files
csv_file_path = '../../data/seeds__network_version.csv'
sql_template_dir = 'input'
output_dir = 'output'

# Get the SQL template file names
sql_template_files = [f for f in os.listdir(sql_template_dir) if f.endswith('.sql')]

# Read the network data from the CSV
with open(csv_file_path, 'r') as f:
    reader = csv.DictReader(f)
    network_data = {row['network_version']: {'root_height': int(row['root_height']), 'node_url': row['node_url'], 'end_height': int(row['end_height'])} for row in reader}

# Loop through the network data and replace the variables in the SQL templates
for network, data in network_data.items():
    # Loop through the SQL template file names
    for sql_template_file in sql_template_files:
        # Define the path to the SQL template file
        sql_template_path = os.path.join(sql_template_dir, sql_template_file)
        # Read the SQL template file
        with open(sql_template_path, 'r') as f:
            sql = f.read()
        # Replace the variables in the SQL template
        sql = sql.replace('NETWORK_NODE_URL', data['node_url'])
        sql = sql.replace('NETWORK_VERSION', network.replace('-', '_'))
        sql = sql.replace('root_height', str(data['root_height']))
        sql = sql.replace('end_height', str(data['end_height']))
        # Replace the NETWORK_VERSION keyword with the network name
        sql_file_name = sql_template_file.replace('NETWORK_VERSION', network.replace('-', '_'))
        # Define the path to the output SQL file
        output_sql_path = os.path.join(output_dir, sql_file_name)
        # Write the modified SQL to the output file
        with open(output_sql_path, 'w') as f:
            f.write(sql)
        print(f"File {output_sql_path} created.")
