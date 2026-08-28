from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check():
    print("Running data quality check+++====+++++")
    # tutaj wstaw właściwą logikę sprawdzania danych
    # np. zapytanie do bazy, walidacja liczby wierszy, itp.

with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:
    check_task = PythonOperator(
        task_id="check",
        python_callable=check,
    )