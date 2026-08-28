ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      -r /requirements.txt
RUN pip install apache-airflow-providers-postgres

COPY dags /opt/airflow/dags
COPY --chmod=0755 docker-entrypoint.sh /opt/airflow/railway-entrypoint.sh

# ⭐ Uruchom kontener jako root, żeby entrypoint mógł zrobić chown na wolumenie
USER root

ENTRYPOINT ["/opt/airflow/railway-entrypoint.sh"]