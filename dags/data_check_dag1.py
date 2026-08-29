from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def check():
    print("Running data quality check+++====+++++")
    

def fetch_measurements():
    hook = PostgresHook(postgres_conn_id="Api_hoy_db") 
    rows = hook.get_records("SELECT * FROM measurements LIMIT 5;")
    for row in rows:
        print(row)
    return rows

def get_days_from_now():
    hook = PostgresHook(postgres_conn_id="Api_hoy_db") 
    rows = hook.get_records(''''SELECT installation_id, timestamp, power
                            FROM measurements
                            WHERE installation_id = 6402
                            AND timestamp >= NOW() - INTERVAL '12 day'
                            ORDER BY timestamp;'''')
    for row in rows:
        print(row)
    return rows


with DAG(
    dag_id="data_quality_check",
    start_date=datetime(2026, 4, 1),
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

    get_days = PythonOperator(
        task_id="get_days_from_now",
        python_callable=get_days_from_now,
    )

    check_task >> fetch_task >> get_days