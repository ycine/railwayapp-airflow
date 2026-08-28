from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def check():
    print("Running data quality check+++====+++++")
    # tutaj wstaw właściwą logikę sprawdzania danych
    # np. zapytanie do bazy, walidacja liczby wierszy, itp.

def fetch_measurements():
    hook = PostgresHook(postgres_conn_id="Api_hoy_db")  # podmień na swój connection_id z Airflow
    rows = hook.get_records("SELECT * FROM measurements LIMIT 5;")
    for row in rows:
        print(row)
    return rows

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

    fetch_task = PythonOperator(
        task_id="fetch_measurements",
        python_callable=fetch_measurements,
    )

    check_task >> fetch_task