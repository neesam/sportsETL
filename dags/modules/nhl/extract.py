from dotenv import load_dotenv
import logging
import os
import requests

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import get_current_context
import pandas as pd

load_dotenv()

# Initialize environment variables

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