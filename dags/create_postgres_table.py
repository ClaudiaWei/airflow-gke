from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime(2020,3,8),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_record_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS record (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL,
            name VARCHAR NOT NULL,
            score INT NOT NULL,
            create_at DATE NOT NULL);
          """,
    )