import os
import pendulum
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "artful-talon-343315")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "airflow-gke")
FILENAME = "record.json"
SQL_QUERY = "select * from record;"
DATASET_NAME = "airflow"
TABLE_NAME = "record"
DEST_TABLE_NAME = "bq_aggregation"

with DAG(
    dag_id="gcs_operator_dag",
    start_date=pendulum.datetime(2022, 3, 14, tz="Asia/Taipei"),
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
    load_into_bq = GCSToBigQueryOperator(
        task_id="load_into_bq",
        bucket=GCS_BUCKET,
        source_objects=['record.json'],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {'name': 'id', 'type': 'INT64', 'mode': 'Required'},
            {'name': 'user_id', 'type': 'INT64', 'mode': 'Required'},
            {'name': 'name', 'type': 'STRING', 'mode': 'Required'},
            {'name': 'score', 'type': 'INT64', 'mode': 'Required'},
            {'name': 'create_at', 'type': 'FLOAT', 'mode': 'Required'},
        ],
        dag=dag,
    )
    bq_aggregation = BigQueryOperator(
        task_id="bq_aggregation",
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        sql='''
            SELECT COUNT(DISTINCT user_id) as count, timestamp_value 
            FROM (
                SELECT user_id, DATE_TRUNC(TIMESTAMP_SECONDS(cast(create_at as int64)), MONTH) AS timestamp_value 
                FROM artful-talon-343315.airflow.record
                ) 
            GROUP BY timestamp_value
            ''',
        destination_dataset_table=f"{DATASET_NAME}.{DEST_TABLE_NAME}",
        dag=dag)

    upload_data_gcs >> load_into_bq >> bq_aggregation