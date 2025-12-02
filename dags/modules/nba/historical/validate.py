from dotenv import load_dotenv
import logging
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

hook = GCSHook(gcp_conn_id=f"{GCP_CONN_ID}")
storage_client = hook.get_conn() 

def validate_wizards():

    context = get_current_context()

    blobs = storage_client.get_bucket(f"{GCS_BUCKET}").list_blobs()

    filenames = [blob.name for blob in blobs]

    stats_filenames_with_prefix = [f"gs://{GCS_BUCKET}/" + blob for blob in filenames if "outcome" not in blob]
    outcome_filenames_with_prefix = [f"gs://{GCS_BUCKET}/" + blob for blob in filenames if "outcome" in blob]

    context['ti'].xcom_push(key='stats_filenames', value=stats_filenames_with_prefix)
    context['ti'].xcom_push(key='outcome_filenames', value=outcome_filenames_with_prefix)

    for file in filenames:
        try:
            file_type = get_parquet(file)
            validate(file_type)
        except Exception as err:
            print(err)

def get_parquet(file):

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")
    blob = bucket.get_blob(f"{file}")

    file_type = ""
    
    try:
        if "outcome" in blob.name:
            blob.download_to_filename(f"{OUTCOME_PARQUET}")
            file_type = "outcome"
            print("Got parquet from bucket:", file)
        else:
            blob.download_to_filename(f"{GAME_PARQUET}")
            file_type = "stats"
            print("Got parquet from bucket:", file)
    except Exception as e:
        print("Something with if statement")
        print(e.__cause__)
    
    print("The file type is:", file_type)

    return file_type

def validate(file_type):

    if file_type == 'stats':
        df = pd.read_parquet(f"{GAME_PARQUET}")
        SportsETLHandler.NBAValidator.validate_stats(df)
    elif file_type == 'outcome':
        df = pd.read_parquet(f"{OUTCOME_PARQUET}")
        SportsETLHandler.NBAValidator.validate_outcome(df)