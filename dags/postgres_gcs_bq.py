import os
from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "artful-talon-343315")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "airflow-gke")
FILENAME = "record"
SQL_QUERY = "select * from record;"

with DAG(
    dag_id="gcs_operator_dag",
    start_date=datetime(2020,3,9),
    schedule_interval="@once",
    catchup=False,
) as dag:
    upload_data_gcs = PostgresToGCSOperator(
        task_id="upload_data", sql=SQL_QUERY, bucket=GCS_BUCKET, filename=FILENAME, gzip=False
    )
    upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="upload_data_with_server_side_cursor",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
    )
    load_into_bq = GCSToBigQueryOperator(
        task_id="load_into_bq",
        bucket=GCS_BUCKET,
        source_objects=[FILENAME],
        destination_project_dataset_table='airflow.gcs_to_bq_table',
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
        schema_fields=[
            {'name': 'id', 'type': 'INT64', 'mode': 'Required'},
            {'name': 'user_id', 'type': 'INT64', 'mode': 'Required'},
            {'name': 'name', 'type': 'STRING', 'mode': 'Required'},
            {'name': 'score', 'type': 'INT64', 'mode': 'Required'},
            {'name': 'create_at', 'type': 'DATE', 'mode': 'Required'},
        ],
    )

    upload_data_gcs >> load_into_bq