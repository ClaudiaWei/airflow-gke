import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

def helloWorld():
    print('Hello World')

with DAG(dag_id="hello_world_dag",
         start_date=pendulum.datetime(2022, 3, 14, tz="Asia/Taipei"),
         schedule_interval="@once",
         catchup=False,
         tags=["example"]
) as dag:
        task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)

task1