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

GAME_CSV = os.getenv("WIZARDS_GAME_CSV")
OUTCOME_CSV = os.getenv("OUTCOME_CSV")
GCS_BUCKET = os.getenv("GCS_BUCKET")
WIZARDS_ID = os.getenv("WIZARDS_ID")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")

# Initialize Google Cloud Platform client 

gcs_hook = GCSHook(gcp_conn_id=f"{GCP_CONN_ID}")
storage_client = gcs_hook.get_conn()

bq_hook = BigQueryHook(gcp_conn_id=f"{GCP_CONN_ID}")
bq_client = bq_hook.get_conn()
bq_cursor = bq_client.cursor()

def ingest_nba():

    context = get_current_context()     
    dates = context['ti'].xcom_pull(key='dates', task_ids='get_dates')

    # Test to see if date is valid for API response
    for date in dates:
        try:
            data_to_load = extract(date)
            convert_to_csv(data_to_load)
            upload_to_gcs(date)
        except:
            continue
    else:
        return


def extract(game_date):

    print("started running extract function")

    get_game_id_api_url = f'https://v1.basketball.api-sports.io/games?date={game_date}&team=161&season=2025-2026'

    # Get game ID and which team won and which team lost
    response = SportsETLHandler.NBAAPI.setup_and_call(get_game_id_api_url)

    print("First response ->>>", response)

    game_id, dict_results = \
        SportsETLHandler.NBAAPI.create_extraction_dict_for_basic_game_info(response)
    
    print("We got the response from the first function")
    
    get_game_stats_api_url = f"https://v1.basketball.api-sports.io/games/statistics/teams?id={game_id}"

    second_response = SportsETLHandler.NBAAPI.setup_and_call(get_game_stats_api_url)

    # Get stats for both teams from the game
    stats_results = SportsETLHandler.NBAAPI.create_extraction_dict_for_game_stats(second_response, dict_results)

    return stats_results
            
def convert_to_csv(data):
    stats_flat_data = []

    # Turn game data to csv
    for d in data[:2]:
        for _, stats in d.items():
            stats_flat_data.append(stats)

    df = pd.DataFrame(stats_flat_data)
    pd.DataFrame.to_csv(df, path_or_buf=f"{GAME_CSV}", index=False)

    outcome_flat_data = []

    # Turn outcome data to csv
    for stats in data[2].values():
        outcome_flat_data.append(stats)

    df = pd.DataFrame(outcome_flat_data)
    pd.DataFrame.to_csv(df, path_or_buf=f"{OUTCOME_CSV}", index=False)

def upload_to_gcs(game_date):

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")

    stats_name_in_gcs_bucket = bucket.blob(f"wizards/game-{game_date}.csv")
    stats_name_in_gcs_bucket.upload_from_filename(f"{GAME_CSV}")

    outcome_name_in_gcs_bucket = bucket.blob(f"wizards/game-{game_date}-outcome.csv")
    outcome_name_in_gcs_bucket.upload_from_filename(f"{OUTCOME_CSV}")

    logging.info("Sucessfully uploaded: " + f"{GCS_BUCKET}/wizards/game-{game_date}.csv")
    logging.info("Sucessfully uploaded: " + f"{GCS_BUCKET}/wizards/game-{game_date}-outcome.csv")