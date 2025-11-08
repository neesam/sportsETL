from airflow.sdk import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import  EmptyOperator

from modules.espn_ingestion import ingest_espn
from modules.nba_ingestion import ingest_nba

with DAG(
    dag_id="sports",
    description="Gets data from sports APIs, loads to staging tables/GCS, performs dbt transformations, creates visualizations",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),  # Manual trigger only
    catchup=False,
) as dag:
    
    ESPN_ingest_and_load_csv = PythonOperator(
        python_callable=ingest_espn,
        task_id="ESPN_ingest_and_load_csv",
    )

    NBA_ingest_and_load_csv = PythonOperator(
        python_callable=ingest_nba,
        task_id="NBA_ingest_and_load_csv"
    )

    ESPN_create_table_from_csv = EmptyOperator(
        task_id="ESPN_create_table_from_csv"
    )

    NBA_create_table_from_csv = EmptyOperator(
        task_id="NBA_create_table_from_csv"
    )

    ESPN_ingest_and_load_csv >> ESPN_create_table_from_csv
    NBA_ingest_and_load_csv >> NBA_create_table_from_csv