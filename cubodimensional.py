from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def transform_load_dw():
    pg = PostgresHook(postgres_conn_id="postgres_dw")
    conn = pg.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer AS
        SELECT customerid, personid, territoryid
        FROM customer_raw;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_product AS
        SELECT p.productid, p.name, c.name AS category
        FROM product_raw p
        LEFT JOIN productsubcategory_raw s ON p.productsubcategoryid = s.productsubcategoryid
        LEFT JOIN productcategory_raw c ON s.productcategoryid = c.productcategoryid;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_sales AS
        SELECT 
            h.salesorderid,
            h.customerid,
            d.productid,
            h.orderdate,
            d.orderqty,
            d.unitprice,
            d.linetotal
        FROM salesorderdetail_raw d
        JOIN salesorderheader_raw h ON d.salesorderid = h.salesorderid;
    """)

    conn.commit()

with DAG(
    "transform_core_dw",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
):
    task = PythonOperator(
        task_id="transform_dw",
        python_callable=transform_load_dw
    )
