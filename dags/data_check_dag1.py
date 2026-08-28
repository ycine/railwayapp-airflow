from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2024, 1, 1),
    schedule="@once",   # tylko to, bez schedule_interval
    catchup=False,
) as dag:
    check_task = PythonOperator(
        task_id="check",
        python_callable=check,
        # nie trzeba przekazywać dag=dag przy użyciu context managera "with DAG(...) as dag"
    )