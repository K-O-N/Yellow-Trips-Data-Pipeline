import os 
import logging 
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator 

from google.cloud import storage 
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Store environmental variables (in your docker container) locally. The second argument of each `.get` is what it will default to if it's empty.
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'nytaxi_data')


# Define a task function and use a python callable to execute it
def upload_to_gcs(bucket, object_name, local_file):
    """
    Uploads a file to the bucket.
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name #The path to your file to upload 
    :param local_file: source path & file-name
    :return:"""

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file) 

# Define default args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2) 
}


# Wrap my task in a definition
def download_task_gcs_upload(dag, dataset_url, bucket, path_to_local_home, paraque_file):
    
    # Define the task for multiple files download
    download_dataset = BashOperator.partial(
        task_id="download_dataset",
        bash_command=(
            "curl -sS {{ params.url }}/{{ params.file }} "
            "> {{ params.local_path }}/{{ params.file }}"
        ),
        dag=dag,
    ).expand(
        params=[
            {
                "file": file,
                "url": dataset_url,
                "local_path": path_to_local_home,
            }
            for file in parquet_file
        ]
    )


# upload to gcs task 
    local_to_gcs_task=PythonOperator.partial(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs, 
        dag=dag,
    ).expand(
        op_kwargs= [ 
            {
            "bucket": BUCKET,
            "object_name": f"raw/{file.replace('.csv', '.parquet')}",
            "local_file": f"{path_to_local_home}/{file}",
        } for file in parquet_file
        ]
    )

    # Tables would be created later
    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=(
            "rm -f "
            + " ".join([f"{path_to_local_home}/{file}" for file in parquet_file])
            
        ),
        dag=dag,
    )
    

    download_dataset >> local_to_gcs_task >> rm_task


# Dag 1
# Specify our dataset
DATES = ["2020-01", "2021-01", "2022-01"]
parquet_file = [f"yellow_tripdata_{d}.parquet" for d in DATES]
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data"

with DAG(
    dag_id="yellow_taxi_data",
    start_date = datetime(2024,1,1),
    end_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  
    default_args=default_args,
    catchup = True,
    max_active_runs=2,
    tags = ['taxi-data-pipeline']    #categorise 
) as dag:
    
    download_task_gcs_upload(
        dag, 
        dataset_url, 
        BUCKET, 
        path_to_local_home, 
        parquet_file
        )


# Dag 2
# Specify our dataset
    parquet_file = ["green_tripdata_2020-01.parquet"]
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/"

with DAG(
    dag_id="green_taxi_data",
    start_date = datetime(2024,1,1),
    end_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  
    default_args=default_args,
    catchup = True,
    max_active_runs=2,
    tags = ['taxi-data-pipeline']    #categorise 
) as dag:
    
    download_task_gcs_upload(
        dag, 
        dataset_url, 
        BUCKET, 
        path_to_local_home, 
        parquet_file
        )

    
# Dag 3 
# Specify our dataset
parquet_file = ["fhv_tripdata_2019-01.parquet"]
dataset_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

with DAG(
    dag_id="fhv_taxi_data",
    start_date = datetime(2024,1,1),
    end_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  
    default_args=default_args,
    catchup = True,
    max_active_runs=2,
    tags = ['taxi-data-pipeline']    #categorise 
) as dag:
    
    download_task_gcs_upload(
        dag, 
        dataset_url, 
        BUCKET, 
        path_to_local_home, 
        parquet_file
        )
    

# Dag 4
# Specify our dataset
    parquet_file = ["taxi_zone_lookup.csv"]
    dataset_url = "https://d37ci6vzurychx.cloudfront.net/misc"

with DAG(
    dag_id="zones_taxi_data",
    start_date = datetime(2024,1,1),
    end_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  
    default_args=default_args,
    catchup = True,
    max_active_runs=2,
    tags = ['taxi-data-pipeline']    #categorise 
) as dag:
    
    download_task_gcs_upload(
        dag, 
        dataset_url, 
        BUCKET, 
        path_to_local_home, 
        parquet_file
        )
