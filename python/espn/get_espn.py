import snowflake
import pandas as pd
import requests as res
import re
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


def upload_to_snowflake(conn, data, table_name):

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data)
    df.columns = [camel_to_snake(col) for col in df.columns]

    # Add a TIMESTAMP column
    df['_INSERTED_TIMESTAMP'] = datetime.now(timezone.utc)

    df.rename(columns={'$REF': 'REF'}, inplace=True)

    # Map pandas data types to Snowflake data types
    dtype_mapping = {
        'object': 'STRING',
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'datetime64[ns]': 'FLOAT', # loaded as unix timestamp. Can cast in SF using :: TIMESTAMP_NTZ
        'bool': 'BOOLEAN',
        'dict': 'VARIANT',
        # Add more mappings if needed
    }

    # Generate the CREATE TABLE statement
    # TODO should change this to IF NOT EXISTS ? Or leave as-is because future pulls overwrite instead of append
    create_table_stmt = 'CREATE OR REPLACE TABLE {} (\n'.format(table_name)
    for col, dtype in df.dtypes.items():
        snowflake_dtype = dtype_mapping.get(str(dtype), 'STRING')
        create_table_stmt += '    "{}" {},\n'.format(col, snowflake_dtype)
    create_table_stmt = create_table_stmt.rstrip(',\n') + '\n);'

    # Execute the CREATE TABLE statement
    conn.cursor().execute(create_table_stmt)

    # Upload the DataFrame to Snowflake
    write_pandas(conn, df, table_name)


def get_espn(base_url, params):
    # paginate through endpoint until no items returned to get urls for each entity
    response = []
    page = 1
    while True:
        r = res.get(f"{base_url}?page={page}", params=params)
        if len(r.json()['items']) == 0:
            break
        response.extend(r.json()['items'])
        page += 1

    # log completion of url request
    print(f"Retrieved {len(response)} items from {base_url}")

    data = []
    errors = []
    for i, url in enumerate(response):
        try:
            r = res.get(url['$ref'])

            # if success, append to data
            if r.status_code == 200:
                data.append(r.json())
            else:
                print(f"Error getting {url['$ref']}: {r.status_code}")
                # append error to list of errors
                errors.append(url['$ref'])

            # log every 100 successful requests
            if i % 100 == 0:
                print(f"Retrieved {i} entities")

        except Exception as e:
            print(f"Error getting {url['$ref']}: {e}")
            # append error to list of errors
            errors.append(url['$ref'])


    # if any errors, write to file
    if len(errors) > 0:
        with open('errors.txt', 'w') as f:
            f.write('\n'.join(errors))

    return data
    

def camel_to_snake(name):
    # Convert camel case to snake case for column naming
    
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)

    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).upper()


def main():
    load_dotenv()
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('USERNAME'),
        authenticator= "externalbrowser",
        account=os.getenv('ACCOUNT'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
        role=os.getenv('ROLE'),
    )

    # teams_url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2023/teams"
    # params = {
    #     "limit": 50
    # }
    # table_name = "ESPN_NFL_TEAMS"

    # data = get_espn(teams_url, params)
    # upload_to_snowflake(conn, data, table_name)

    athletes_url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes"
    params = {
        "limit": 1000,
        "active": "true"
    }
    table_name = "ESPN_NFL_ATHLETES"

    data = get_espn(athletes_url, params)
    upload_to_snowflake(conn, data, table_name)

    # Close the connection
    conn.close()

    return True

if __name__ == '__main__':
    main()
