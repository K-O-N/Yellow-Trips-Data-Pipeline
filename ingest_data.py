#!/usr/bin/env python
# coding: utf-8

import os 
import requests
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    url = params.url
    table_name = params.table_name
    db = params.db
    port = params.port
    parquet_name = 'output.parquet'

    try:
        print(f"Downloading data from: {url} to {parquet_name}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(parquet_name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded data to {parquet_name}")

        # Load data to database
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

        # Read data
        df = pd.read_parquet(parquet_name)

        ##print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))

        # create table
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        # Define chunk size
        chunk_size = 100000

        # Insert the DataFrame in chunks
        for i in range(0, len(df), chunk_size):
            df_chunk = df[i:i + chunk_size]
            df_chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',  # Append data to the table
                index=False,          # Don't write the DataFrame index to the SQL table
                chunksize=chunk_size  # explicitly set chunksize in to_sql
            )
            print(f"Inserted chunk {i // chunk_size + 1} of {len(df) // chunk_size + 1}") # give a progress

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
    except FileNotFoundError:
        print(f"Error: The file '{parquet_name}' was not found after download.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data to Postgres Database')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='table name to write results to')
    parser.add_argument('--url', help='parquet file url')

    args = parser.parse_args()

    main(args)

## python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_data --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
## python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_data --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"