import os

from airflow.sdk import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import get_current_context
import pendulum

from dotenv import load_dotenv

from modules.espn.ingestion import ingest_espn
from modules.nba.team.extract import ingest_nba
from modules.nba.team.validate import validate_wizards

load_dotenv()

RUN_DATES = [
    "2025-10-22",
    "2025-10-24",
    "2025-10-26",
    "2025-10-28",
    "2025-10-30",
    "2025-11-01",
    "2025-11-03",
    "2025-11-05",
    "2025-11-07",
    "2025-11-08",
    "2025-11-10",
    "2025-11-12",
    "2025-11-16",
    "2025-11-19",
    "2025-11-21",
    "2025-11-22",
    "2025-11-25",
    "2025-11-28",
    "2025-12-01",
    "2025-12-02",
    "2025-12-04",
    "2025-12-06",
    "2025-12-17",
    "2025-12-20",
    "2025-12-21",
    "2025-12-23",
    "2025-12-26",
    "2025-12-28",
    "2025-12-29",
    "2025-12-31",
    "2026-01-02",
    "2026-01-04",
    "2026-01-06",
    "2026-01-07",
    "2026-01-09",
    "2026-01-11",
    "2026-01-14",
    "2026-01-16",
    "2026-01-17",
    "2026-01-19",
    "2026-01-22",
    "2026-01-24",
    "2026-01-27",
    "2026-01-29",
    "2026-01-30",
    "2026-02-01",
    "2026-02-03",
    "2026-02-05",
    "2026-02-07",
    "2026-02-08",
    "2026-02-11",
    "2026-02-19",
    "2026-02-20",
    "2026-02-22",
    "2026-02-24",
    "2026-02-26",
    "2026-02-28",
    "2026-03-02",
    "2026-03-03",
    "2026-03-05",
    "2026-03-08",
    "2026-03-10",
    "2026-03-12",
    "2026-03-14",
    "2026-03-16",
    "2026-03-17",
    "2026-03-19",
    "2026-03-21",
    "2026-03-22",
    "2026-03-25",
    "2026-03-27",
    "2026-03-29",
    "2026-03-30",
    "2026-04-01",
    "2026-04-04",
    "2026-04-05",
    "2026-04-07",
    "2026-04-09",
    "2026-04-10",
    "2026-04-12"
]

# Load env vars

BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GCP_PROJECT = os.getenv("GCP_PROJECT")
WIZARDS_GAMES_STATS_TABLE = os.getenv("WIZARDS_GAMES_STATS_TABLE")
WIZARDS_GAME_GCS_CSV = os.getenv("WIZARDS_GAME_GCS_CSV")
GCS_BUCKET = os.getenv("GCS_BUCKET")

with DAG(
    dag_id="sports",
    description="Gets data from sports APIs, loads to staging tables/GCS, performs dbt transformations, creates visualizations",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Denver"),
    schedule='0 22 * * *',
    catchup=False,
) as dag:
        # def check_release_date(**context):
    #     logical_date = context['logical_date']
    
    #     # Convert to your timezone
    #     local_date = logical_date.in_timezone("America/Denver")
        
    #     # Format as string to compare with your list
    #     local_date_str = local_date.strftime('%Y-%m-%d')
    #     logical_date_str = logical_date.strftime('%Y-%m-%d')
        
    #     print(f"UTC execution_date: {logical_date}")
    #     print(f"Local date (America/Denver): {local_date_str}")
    #     print(f"Should run: {local_date_str in RUN_DATES}")

    #     context = get_current_context()
        
    #     if local_date_str in RUN_DATES:
    #         context['ti'].xcom_push('api_date', logical_date_str)
    #         context['ti'].xcom_push('local_date', local_date_str)
    #         return True
    #     else:
    #         return False

    def check_release_date_static():
        context = get_current_context() 
        context['ti'].xcom_push('local_date', "2025-11-12")
        context['ti'].xcom_push('api_date', "2025-11-13")
        return True

    skip_if_not_release_date = ShortCircuitOperator(
        task_id='skip_if_not_release_date',
        python_callable=check_release_date_static
    )
    
    # ESPN_ingest_and_load_csv = PythonOperator(
    #     python_callable=ingest_espn,
    #     task_id="ESPN_ingest_and_load_csv",
    # )

    NBA_ingest_and_load_csv = PythonOperator(
        python_callable=ingest_nba,
        task_id="NBA_ingest_and_load_csv",
    )

    NBA_validate_team_game_stats = PythonOperator(
        python_callable=validate_wizards,
        task_id="NBA_validate_team_game_stats"
    )

    NBA_create_table_from_csv = BigQueryInsertJobOperator(
        task_id="NBA_load_gcs_to_bq",
        configuration={
            "load": {
                "sourceUris": [WIZARDS_GAME_GCS_CSV + "-{{ ti.xcom_pull(task_ids='skip_if_not_release_date', key='local_date') }}.csv"],
                "destinationTable": {
                    "projectId": f"{GCP_PROJECT}",
                    "datasetId": f"{BQ_DATASET}",
                    "tableId": f"{WIZARDS_GAMES_STATS_TABLE}",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    skip_if_not_release_date >> NBA_ingest_and_load_csv
    NBA_ingest_and_load_csv >> NBA_validate_team_game_stats
    NBA_validate_team_game_stats >> NBA_create_table_from_csv