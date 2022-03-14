import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=pendulum.datetime(2022, 3, 14, tz="Asia/Taipei"),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_record_table = PostgresOperator(
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

    feed_record_table = PostgresOperator(
        task_id="feed_record_table",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '1', 'GameA', '66', '2022-02-20');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '2', 'GameB', '75', '2022-02-21');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '1', 'GameC', '89', '2022-02-22');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '3', 'GameA', '30', '2022-03-02');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '3', 'GameC', '92', '2022-02-28');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '3', 'GameD', '73', '2022-03-05');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '4', 'GameB', '68', '2022-03-05');
            INSERT INTO record (user_id, name, score, create_at)
            VALUES ( '5', 'GameC', '95', '2022-02-27');
            """,
    )
    
    create_record_table >> feed_record_table