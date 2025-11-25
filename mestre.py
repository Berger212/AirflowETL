from airflow import DAG
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    "pipeline_master",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
):

    extract = TriggerDagRunOperator(task_id="run_extract", trigger_dag_id="extract_mysql")
    transform = TriggerDagRunOperator(task_id="run_transform", trigger_dag_id="transform_core_dw")
    metrics = TriggerDagRunOperator(task_id="run_metrics", trigger_dag_id="load_metrics")
    indicators = TriggerDagRunOperator(task_id="run_indicators", trigger_dag_id="update_business_indicators")
    validation = TriggerDagRunOperator(task_id="run_validation", trigger_dag_id="validation_reporting")

    extract >> transform >> metrics >> indicators >> validation
