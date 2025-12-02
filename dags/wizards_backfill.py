from datetime import date, timedelta
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

# Load env vars

GCP_CONN_ID = os.getenv("GCP_CONN_ID")

GCP_PROJECT = os.getenv("GCP_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")

WIZARDS_GAMES_STATS_TABLE = os.getenv("WIZARDS_GAMES_STATS_TABLE")
WIZARDS_GAMES_OUTCOME_TABLE = os.getenv("WIZARDS_GAMES_OUTCOME_TABLE")
WIZARDS_CRAWL_LOG_TABLE = os.getenv("WIZARDS_CRAWL_LOG_TABLE")

WIZARDS_GAME_GCS_PARQUET = os.getenv("WIZARDS_GAME_GCS_PARQUET")
GCS_BUCKET = os.getenv("GCS_BUCKET")

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")

bq_hook = BigQueryHook(gcp_conn_id=f"{GCP_CONN_ID}")
bq_client = bq_hook.get_conn()
bq_cursor = bq_client.cursor()

with DAG(
    dag_id="nba_backfill",
    description="Backfills data for NBA",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Denver"),
    schedule=None,
    catchup=False,
) as dag:

    # def get_candidate_dates():
    #     context = get_current_context()
    #     delta = abs(date.fromisoformat("2025-10-20") - date.today()).days
    #     context["ti"].xcom_push(
    #         "dates",
    #         [
    #             (date.fromisoformat("2025-10-20") + timedelta(days=i)).isoformat()
    #             for i in range(delta + 1)
    #         ],
    #     )

    # get_dates = PythonOperator(task_id="get_dates", python_callable=get_candidate_dates)

    def check_for_dates_in_table():

        context = get_current_context()

        bq_cursor.execute(
            f"""
                                SELECT game_id, game_date
                                FROM {GCP_PROJECT}.{BQ_DATASET}.{WIZARDS_CRAWL_LOG_TABLE}
                                WHERE NOT extracted AND game_id <> 'None'
                                GROUP BY game_id, game_date
                           """
        )
        results = {row[0] for row in bq_cursor.fetchall()}

        if results:
            context["ti"].xcom_push("dates_to_fill", results)
            return True
        else:
            return False

    check_for_dates = ShortCircuitOperator(
        python_callable=check_for_dates_in_table, task_id="check_for_dates"
    )

    NBA_ingest_and_load_parquet = PythonOperator(
        python_callable=ingest_nba,
        task_id="NBA_ingest_and_load_parquet",
    )

    NBA_validate_team_game_stats = PythonOperator(
        python_callable=validate_wizards, task_id="NBA_validate_team_game_stats"
    )

    def build_stats_table_load_config(**context):

        context = get_current_context()
        filenames = context["ti"].xcom_pull(
            task_ids="NBA_validate_team_game_stats", key="stats_filenames"
        )

        return {
            "load": {
                "sourceUris": filenames,
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": WIZARDS_GAMES_STATS_TABLE,
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
            }
        }

    NBA_load_stats_table_from_parquet = BigQueryInsertJobOperator(
        task_id="NBA_load_stats_gcs_to_bq",
        configuration=build_stats_table_load_config,
        gcp_conn_id=f"{GCP_CONN_ID}",
    )

    def build_outcomes_table_load_config(**context):

        context = get_current_context()

        filenames = context["ti"].xcom_pull(
            task_ids="NBA_validate_team_game_stats", key="outcome_filenames"
        )

        return {
            "load": {
                "sourceUris": filenames,
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "wizards_game_outcomes",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
            }
        }

    NBA_load_outcomes_table_from_parquet = BigQueryInsertJobOperator(
        task_id="NBA_load_outcomes_gcs_to_bq",
        configuration=build_outcomes_table_load_config,
        gcp_conn_id=f"{GCP_CONN_ID}",
    )

    check_for_dates >> NBA_ingest_and_load_parquet
    NBA_ingest_and_load_parquet >> NBA_validate_team_game_stats
    NBA_validate_team_game_stats >> [
        NBA_load_outcomes_table_from_parquet,
        NBA_load_stats_table_from_parquet,
    ]
