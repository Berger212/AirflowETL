from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

TABLES = [
    "SalesOrderHeader",
    "SalesOrderDetail",
    "Customer",
    "Product",
    "ProductCategory",
    "ProductSubcategory"
]

def extract_table(table):
    mysql = MySqlHook(mysql_conn_id="mysql_adventureworks")
    postgres = PostgresHook(postgres_conn_id="postgres_dw")

    df = mysql.get_pandas_df(f"SELECT * FROM {table}")
    df.to_sql(table.lower() + "_raw", postgres.get_sqlalchemy_engine(), if_exists="replace", index=False)

def extract_all():
    for table in TABLES:
        extract_table(table)

with DAG(
    "extract_mysql",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
):
    task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_all
    )