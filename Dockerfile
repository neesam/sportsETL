FROM apache/airflow:3.1.2

RUN pip install --no-cache-dir google-cloud-storage google-cloud-bigquery dbt-core dbt-bigquery apache-airflow-providers-google nba_api