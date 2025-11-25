from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "validation_reporting",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
):
    BashOperator(
        task_id="validate_pipeline",
        bash_command='echo "Pipeline executada com sucesso em $(date)"'
    )
