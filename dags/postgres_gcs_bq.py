import os
from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "artful-talon-343315")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "airflow-gke")
FILENAME = "record.json"
SQL_QUERY = "select * from record;"
DATASET_NAME = "airflow"
TABLE_NAME = "record"

with DAG(
    dag_id="gcs_operator_dag",
    start_date=datetime(2020,3,9),
    schedule_interval="@once",
    catchup=False,
) as dag:
    upload_data_gcs = PostgresToGCSOperator(
        task_id="upload_data", 
        sql=SQL_QUERY, 
        bucket=GCS_BUCKET, 
        filename=FILENAME, 
        gzip=False
    )

    upload_data_gcs