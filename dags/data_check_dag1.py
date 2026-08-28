from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def check():
    print("DATA QUALITY CHECK - działa!")


with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2026, 8, 28),
    schedule=None,
    catchup=False,
) as dag:

    check_task = PythonOperator(
        task_id="check",
        python_callable=check,
    )
