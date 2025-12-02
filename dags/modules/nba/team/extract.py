from datetime import date, timedelta
from dotenv import load_dotenv
import logging
import os

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import get_current_context
import pandas as pd

from classes.sports_api_handlers import SportsETLHandler

load_dotenv()

# Initialize environment variables

GAME_PARQUET = os.getenv("WIZARDS_GAME_PARQUET")
OUTCOME_PARQUET = os.getenv("OUTCOME_PARQUET")
GCS_BUCKET = os.getenv("GCS_BUCKET")
WIZARDS_ID = os.getenv("WIZARDS_ID")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")

# Initialize Google Cloud Platform client

hook = GCSHook(gcp_conn_id="google_cloud_default")
storage_client = hook.get_conn()

bq_hook = BigQueryHook(gcp_conn_id=f"{GCP_CONN_ID}")
bq_client = bq_hook.get_client()


def ingest_nba():
    api_date = date.today().strftime("%Y-%m-%d")
    local_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    data_to_load = extract(api_date)
    convert_to_parquet(data_to_load)
    upload_to_gcs(local_date)


def extract(game_date):

    get_game_id_api_url = f"https://v1.basketball.api-sports.io/games?date={game_date}&team=161&season=2025-2026"

    response = SportsETLHandler.NBAAPI.setup_and_call(get_game_id_api_url)

    if response["response"]:

        game_id, dict_results = (
            SportsETLHandler.NBAAPI.create_extraction_dict_for_game_info(response)
        )

    else:
        SportsETLHandler.NBAAPI.upload_game_processing_info_to_bigquery(
            game_date, bq_client
        )
        return None

    # Get stats for both teams from the game

    get_game_stats_api_url = (
        f"https://v1.basketball.api-sports.io/games/statistics/teams?id={game_id}"
    )

    second_response = SportsETLHandler.NBAAPI.setup_and_call(get_game_stats_api_url)

    if not second_response["response"]:
        SportsETLHandler.NBAAPI.upload_game_processing_info_to_bigquery(
            game_date, bq_client, game_id=game_id
        )

    # Get stats for both teams from the game
    stats_results = SportsETLHandler.NBAAPI.create_extraction_dict_for_game_stats(
        second_response, dict_results
    )

    SportsETLHandler.NBAAPI.upload_game_processing_info_to_bigquery(
        game_date, bq_client, extracted=True, game_id=game_id
    )

    return stats_results


def convert_to_parquet(data):
    stats_flat_data = []

    # Turn game data to parquet
    for d in data[:2]:
        for _, stats in d.items():
            stats_flat_data.append(stats)

    df = pd.DataFrame(stats_flat_data)
    pd.DataFrame.to_parquet(df, path=f"{GAME_PARQUET}", index=False)

    outcome_flat_data = []

    # Turn outcome data to parquet
    for stats in data[2].values():
        outcome_flat_data.append(stats)

    df = pd.DataFrame(outcome_flat_data)
    pd.DataFrame.to_parquet(df, path=f"{OUTCOME_PARQUET}", index=False)


def upload_to_gcs(game_date):

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")

    stats_name_in_gcs_bucket = bucket.blob(f"wizards/game-{game_date}.parquet")
    stats_name_in_gcs_bucket.upload_from_filename(f"{GAME_PARQUET}")

    outcome_name_in_gcs_bucket = bucket.blob(
        f"wizards/game-{game_date}-outcome.parquet"
    )
    outcome_name_in_gcs_bucket.upload_from_filename(f"{OUTCOME_PARQUET}")

    logging.info(
        "Sucessfully uploaded: " + f"{GCS_BUCKET}/wizards/game-{game_date}.parquet"
    )
    logging.info(
        "Sucessfully uploaded: "
        + f"{GCS_BUCKET}/wizards/game-{game_date}-outcome.parquet"
    )
