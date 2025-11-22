import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import get_current_context
import pendulum

from dotenv import load_dotenv

from modules.nba.historical.extract import ingest_nba
from modules.nba.historical.validate import validate_wizards

load_dotenv()

dates = [
    "2025-10-22", "2025-10-23", "2025-10-24", "2025-10-25", "2025-10-26",
    "2025-10-27", "2025-10-28", "2025-10-29", "2025-10-30", "2025-10-31",
    "2025-11-01", "2025-11-02", "2025-11-03", "2025-11-04", "2025-11-05",
    "2025-11-06", "2025-11-07", "2025-11-08", "2025-11-09", "2025-11-10",
    "2025-11-11", "2025-11-12", "2025-11-13", "2025-11-14", "2025-11-15",
    "2025-11-16", "2025-11-17", "2025-11-18"
]
# Load env vars

BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GCP_PROJECT = os.getenv("GCP_PROJECT")
WIZARDS_GAMES_STATS_TABLE = os.getenv("WIZARDS_GAMES_STATS_TABLE")
WIZARDS_GAMES_OUTCOME_TABLE = os.getenv("WIZARDS_GAMES_OUTCOME_TABLE")
WIZARDS_GAME_GCS_CSV = os.getenv("WIZARDS_GAME_GCS_CSV")
GCS_BUCKET = os.getenv("GCS_BUCKET")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")

bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
bq_client = bq_hook.get_conn()
bq_cursor = bq_client.cursor()

with DAG(
    dag_id="nba_backfill",
    description="Backfills data for NBA",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Denver"),
    schedule=None,
    catchup=False,
) as dag:

    def get_all_dates_up_to_current_date():    

        context = get_current_context()
        context['ti'].xcom_push('dates', dates)

    get_dates = PythonOperator(
        task_id="get_dates",
        python_callable=get_all_dates_up_to_current_date
    )

    def check_for_dates_in_table():

        bq_cursor.execute(f"SELECT date FROM {GCP_PROJECT}.{BQ_DATASET}.{WIZARDS_GAMES_OUTCOME_TABLE}")
        results = bq_cursor.fetchall()

        return True if results else False

    check_for_dates = ShortCircuitOperator(
        python_callable=check_for_dates_in_table,
        task_id="check_for_dates"
    )

    NBA_ingest_and_load_csv = PythonOperator(
        python_callable=ingest_nba,
        task_id="NBA_ingest_and_load_csv",
    )

    NBA_validate_team_game_stats = PythonOperator(
        python_callable=validate_wizards,
        task_id="NBA_validate_team_game_stats"
    )

    def build_stats_table_load_config(**context):

        context = get_current_context()
        filenames = context['ti'].xcom_pull(
            task_ids='NBA_validate_team_game_stats', 
            key='stats_filenames'
        )
        
        return {
            "load": {
                "sourceUris": filenames,
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": WIZARDS_GAMES_STATS_TABLE,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
                "skipLeadingRows": 1,
            }
        }

    NBA_load_stats_table_from_csv = BigQueryInsertJobOperator(
        task_id="NBA_load_stats_gcs_to_bq",
        configuration=build_stats_table_load_config,
        gcp_conn_id=f"{GCP_CONN_ID}",
    )

    def build_outcomes_table_load_config(**context):

        context = get_current_context()

        filenames = context['ti'].xcom_pull(
            task_ids='NBA_validate_team_game_stats', 
            key='outcome_filenames'
        )
        
        return {
            "load": {
                "sourceUris": filenames,
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "wizards_game_outcomes",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
                "skipLeadingRows": 1,
            }
        }

    NBA_load_outcomes_table_from_csv = BigQueryInsertJobOperator(
        task_id="NBA_load_outcomes_gcs_to_bq",
        configuration=build_outcomes_table_load_config,
        gcp_conn_id=f"{GCP_CONN_ID}",
    )

    # dbt_run_and_test = BashOperator(
    #     task_id='dbt_run_and_test',
    #     bash_command='''
    #     cd {{ params.dbt_project_dir }} && \
    #     dbt build 
    #     ''',
    #     params={
    #         'dbt_project_dir': DBT_PROJECT_DIR
    #     }
    # )

    get_dates >> check_for_dates
    check_for_dates >> NBA_ingest_and_load_csv
    NBA_ingest_and_load_csv >> NBA_validate_team_game_stats
    NBA_validate_team_game_stats >> [NBA_load_outcomes_table_from_csv, NBA_load_stats_table_from_csv]
    # NBA_create_table_from_csv >> dbt_run_and_test
