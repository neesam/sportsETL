from dotenv import load_dotenv
import os

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import get_current_context
import pandas as pd

from classes.sports_api_handlers import SportsETLHandler

load_dotenv()

# Initialize environment variables

GCS_BUCKET = os.getenv("GCS_BUCKET")
WIZARDS_ID = os.getenv("WIZARDS_ID")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")
OUTCOME_PARQUET = os.getenv("OUTCOME_PARQUET")
GAME_PARQUET = os.getenv("WIZARDS_GAME_PARQUET")

# Initialize Google Cloud Platform client

hook = GCSHook(gcp_conn_id="google_cloud_default")
storage_client = hook.get_conn()


def validate_wizards():
    context = get_current_context()
    local_date = context["ti"].xcom_pull(
        key="local_date", task_ids="skip_if_not_release_date"
    )

    file_type = get_parquet(local_date)
    validate(file_type)


def get_parquet(date):

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")
    blobs = bucket.list_blobs()

    files = [blob.name for blob in blobs if date in blob.name]

    file_type = ""

    try:
        for file in files:
            if "outcome" in file:
                file.download_to_filename(f"{OUTCOME_PARQUET}")
                file_type = "outcome"
                print("Got outcome parquet from bucket:", file)
            else:
                file.download_to_filename(f"{GAME_PARQUET}")
                file_type = "stats"
                print("Got stats parquet from bucket:", file)
    except Exception as e:
        print(e)

    return file_type


def validate(file_type):

    if file_type == "stats":
        df = pd.read_parquet(f"{GAME_PARQUET}")
        SportsETLHandler.NBAValidator.validate_stats(df)
    elif file_type == "outcome":
        df = pd.read_parquet(f"{OUTCOME_PARQUET}")
        SportsETLHandler.NBAValidator.validate_outcome(df)
