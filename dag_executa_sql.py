from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
):

    test_query = PostgresOperator(
        task_id="select_test",
        postgres_conn_id="postgres_default",  # Nome da conexão configurada
        sql="SELECT * FROM pg_tables LIMIT 5;"
    )
    
    test_query