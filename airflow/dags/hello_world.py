from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def start_pipeline():
    print("Flight pipeline started.")


with DAG(
    dag_id="hello_world",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial"],
) as dag:

    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )