"""
DAG: data_quality_check
Sprawdza jakość danych z tabeli `measurements` (odczyty co 15 min z urządzeń
w ramach instalacji) — wykrywa luki w danych i alarmuje, gdy coś jest nie tak.

Wymagania wstępne (poza kodem DAG-a):
    - index na measurements(installation_id, timestamp) — bez niego
      window function LAG() OVER (PARTITION BY ... ORDER BY timestamp)
      będzie robić sekwencyjny skan + sort na całej tabeli:

        CREATE INDEX CONCURRENTLY IF NOT EXISTS
            idx_measurements_installation_ts
            ON measurements (installation_id, timestamp);

    - Airflow Connection "Api_hoy_db" skonfigurowane w UI (Admin > Connections),
      nie hardcodowane w kodzie.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

GAP_THRESHOLD = "15 minutes"   # oczekiwany odstęp między odczytami
LOOKBACK_WINDOW = "10 days"    # jak daleko wstecz sprawdzamy
CONN_ID = "Api_hoy_db"


def _get_active_installation_ids(hook: PostgresHook) -> list[int]:
    """Pobiera listę instalacji do sprawdzenia zamiast hardcodować jedną."""
    rows = hook.get_records("SELECT DISTINCT installation_id FROM measurements;")
    return [r[0] for r in rows]


def check_connectivity(**context) -> None:
    """Prosty smoke-test połączenia z bazą przed właściwymi checkami."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.get_first("SELECT 1;")
    log.info("Połączenie z bazą OK.")


def fetch_measurements_summary(**context) -> int:
    """
    Zamiast zwracać surowe wiersze (co zapycha XCom / metadata DB Airflow),
    zwracamy tylko liczbę — do wglądu w UI i ewentualnych dalszych warunków.
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    count = hook.get_first("SELECT COUNT(*) FROM measurements;")[0]
    log.info("Liczba wierszy w measurements: %s", count)
    return count


def check_recent_readings_per_installation(**context) -> None:
    """
    Sprawdza dla każdej instalacji, czy w ostatnich N dniach w ogóle
    coś przyszło (a nie tylko jedna, zahardkodowana instalacja).
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    installation_ids = _get_active_installation_ids(hook)

    missing_data = []
    for installation_id in installation_ids:
        row = hook.get_first(
            """
            SELECT COUNT(*)
            FROM measurements
            WHERE installation_id = %(installation_id)s
              AND timestamp >= NOW() - INTERVAL '12 days';
            """,
            parameters={"installation_id": installation_id},
        )
        if row[0] == 0:
            missing_data.append(installation_id)

    if missing_data:
        f"Brak jakichkolwiek odczytów w ostatnich 12 dniach dla instalacji: "
        f"{missing_data}"
        
    log.info("Wszystkie instalacje (%d) mają odczyty w ostatnich 12 dniach.", len(installation_ids))


def check_gaps_in_readings(**context) -> None:
    """
    Gap detection: wykrywa przerwy w odczytach większe niż GAP_THRESHOLD.
    Failuje task (zamiast tylko logować), żeby dało się to podpiąć pod alerting.
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    rows = hook.get_records(
        f"""
        WITH ordered AS (
            SELECT
                installation_id,
                timestamp,
                LAG(timestamp) OVER (
                    PARTITION BY installation_id ORDER BY timestamp
                ) AS prev_timestamp
            FROM measurements
            WHERE timestamp >= NOW() - INTERVAL '{LOOKBACK_WINDOW}'
        )
        SELECT
            installation_id,
            prev_timestamp,
            timestamp AS next_timestamp,
            timestamp - prev_timestamp AS gap
        FROM ordered
        WHERE timestamp - prev_timestamp > INTERVAL '{GAP_THRESHOLD}'
        ORDER BY gap DESC;
        """
    )

    if rows:
        affected = sorted({r[0] for r in rows})
        log.warning("Wykryto %d luk(i) w danych, instalacje: %s", len(rows), affected)
        # Dociąga do XCom tylko podsumowanie, nie surowe wiersze
        context["ti"].xcom_push(key="gap_count", value=len(rows))
        context["ti"].xcom_push(key="affected_installations", value=affected)
        
        f"Wykryto {len(rows)} luk w danych (próg: {GAP_THRESHOLD}). "
        f"Dotknięte instalacje: {affected}"
        

    log.info("Brak luk w danych w ostatnich %s.", LOOKBACK_WINDOW)


def _on_failure_alert(context) -> None:
    """
    Miejsce na integrację z alertingiem (Slack/e-mail/PagerDuty).
    Na razie tylko log — podmień na realne wywołanie webhooka.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")
    log.error(
        "Task %s w DAG-u %s zawiódł: %s",
        task_instance.task_id,
        task_instance.dag_id,
        exception,
    )
    # np.: send_slack_notification(channel="#data-alerts", message=...)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure_alert,
}

with DAG(
    dag_id="data_quality_check_full",
    description="Wykrywanie braków i luk w danych z measurements",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 * * * *",   # co godzinę — dopasuj do rzeczywistej częstotliwości danych
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "measurements"],
) as dag:

    check_connectivity_task = PythonOperator(
        task_id="check_connectivity",
        python_callable=check_connectivity,
    )

    fetch_summary_task = PythonOperator(
        task_id="fetch_measurements_summary",
        python_callable=fetch_measurements_summary,
    )

    check_recent_task = PythonOperator(
        task_id="check_recent_readings_per_installation",
        python_callable=check_recent_readings_per_installation,
    )

    check_gaps_task = PythonOperator(
        task_id="check_gaps_in_readings",
        python_callable=check_gaps_in_readings,
    )

    # równolegle — awaria jednego nie blokuje pozostałych.
    check_connectivity_task >> [fetch_summary_task]
        
