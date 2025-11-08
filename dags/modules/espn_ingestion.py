from datetime import date
from dotenv import load_dotenv
import logging
import os
import requests

from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd

load_dotenv()

# Initialize environment variables

GCS_BUCKET = os.getenv("GCS_BUCKET")
GAMES_CSV = os.getenv("NFL_GAMES_CSV")

# Initialize Google Cloud Platform client 

hook = GCSHook(gcp_conn_id="google_cloud_default")
storage_client = hook.get_conn() 

# Retrieve data from ESPN API

def ingest_espn():

    games = ingestion()
    convert_to_csv(games)
    upload_to_gcs()

def ingestion():

    try:
        req = requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard")
        req = req.json()
        logging.info("Sucessfully decoded the JSON response")
    except:
        logging.error("Couldn't get the data")

    events = req['events']

    games = []
    teams = []

    for game in range(len(events)):
        games.append(events[game]['competitions'][0])
        teams.append(events[game]['competitions'][0]['competitors'])

    for i in games:
        i.pop('uid')
        i.pop('attendance')
        i.pop('type')
        i.pop('timeValid')
        i.pop('playByPlayAvailable')
        i.pop('recent')
        i.pop('highlights')
        i.pop('startDate')
        i.pop('format')

    logging.info(games)

    return games

# Output games list to csv with Pandas

def convert_to_csv(games_list):

    df = pd.DataFrame(games_list)
    pd.DataFrame.to_csv(df, path_or_buf=f"{GAMES_CSV}")

def upload_to_gcs():

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")
    object_name_in_gcs_bucket = bucket.blob(f"NFL/games-{date.today().isoformat()}.csv")
    object_name_in_gcs_bucket.upload_from_filename(f"{GAMES_CSV}")