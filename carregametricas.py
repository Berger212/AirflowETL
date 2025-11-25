from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_metrics():
    pg = PostgresHook(postgres_conn_id="postgres_dw")
    cursor = pg.get_conn().cursor()

    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_revenue AS
        SELECT date_trunc('month', orderdate) AS month, SUM(linetotal) AS total_revenue
        FROM fact_sales
        GROUP BY 1;
    """)

    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_best_products AS
        SELECT productid, SUM(linetotal) AS revenue
        FROM fact_sales
        GROUP BY productid
        ORDER BY revenue DESC
        LIMIT 10;
    """)

    pg.get_conn().commit()

with DAG(
    "load_metrics",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
):
    PythonOperator(
        task_id="generate_metrics",
        python_callable=load_metrics
    )
