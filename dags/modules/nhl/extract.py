from dotenv import load_dotenv
import logging
import os
import requests

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import get_current_context
import pandas as pd

from classes.nba_api import NBA_API

load_dotenv()

# Initialize environment variables

GAME_CSV = os.getenv("WIZARDS_GAME_CSV")
GCS_BUCKET = os.getenv("GCS_BUCKET")
WIZARDS_ID = os.getenv("WIZARDS_ID")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")

# Initialize Google Cloud Platform client 

gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
storage_client = gcs_hook.get_conn()

bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
bq_client = bq_hook.get_conn()
bq_cursor = bq_client.cursor()

def main():
    pass

def extract():

    res = requests.get("https://api-web.nhle.com/v1/gamecenter/2023020204/boxscore")

    res = res.json()

    print(res)