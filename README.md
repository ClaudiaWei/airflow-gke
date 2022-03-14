# airflow-gke

- `hello_world_dag.py`  
The hello_world dag for testing.

- `feed.py`  
The feed dag which contains 2 tasks:
  - Create record table.
  - Insert data into record table.

- `postgres_gcs_bq.py`  
The postgres_gcs_bq dag which contains 3 tasks:
  - Postgres data to google cloud storage.
  - BigQuery load data from google cloud storage data.
  - BigQuery execute query aggregation.
