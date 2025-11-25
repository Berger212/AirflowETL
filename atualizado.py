from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def update_indicators():
    pg = PostgresHook(postgres_conn_id="postgres_dw")
    cursor = pg.get_conn().cursor()

    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ticket_medio AS
        SELECT AVG(linetotal) AS avg_ticket
        FROM fact_sales;
    """)

    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ltv AS
        SELECT customerid, SUM(linetotal) AS lifetime_value
        FROM fact_sales GROUP BY customerid;
    """)

    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_churn AS
        SELECT customerid,
        CASE WHEN max(orderdate) < NOW() - INTERVAL '120 days'
        THEN 1 ELSE 0 END AS churn_probability
        FROM fact_sales GROUP BY customerid;
    """)

    pg.get_conn().commit()

with DAG(
    "update_business_indicators",
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False
):
    PythonOperator(
        task_id="process_indicators",
        python_callable=update_indicators
    )