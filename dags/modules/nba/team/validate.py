from dotenv import load_dotenv
import os

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import get_current_context
import pandas as pd

load_dotenv()

# Initialize environment variables

GCS_BUCKET = os.getenv("GCS_BUCKET")
WIZARDS_ID = os.getenv("WIZARDS_ID")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")

# Initialize Google Cloud Platform client 

hook = GCSHook(gcp_conn_id="google_cloud_default")
storage_client = hook.get_conn() 

def validate_wizards():
    context = get_current_context()
    local_date = context['ti'].xcom_pull(key="local_date", task_ids="skip_if_not_release_date") 

    get_csv(local_date)
    validate()

def get_csv(date):

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")
    blob = bucket.get_blob(f"wizards/game-{date}.csv")
    blob.download_to_filename("/tmp/wiz.csv")

def validate():

    df = pd.read_csv("/tmp/wiz.csv", header=0)
    
    expected_cols = ['home_or_away', 'game_id', 'outcome', 'field_goals_made',
                    'field_goals_attempts', 'field_goals_pct', 'threes_made',
                    'threes_attempts', 'threes_pct', 'free_throws_made',
                    'free_throw_attempts', 'free_throw_pct', 'rebounds_total',
                    'rebounds_off', 'rebounds_def', 'assists_total', 'steals_total',
                    'blocks_total', 'turnovers_total', 'personal_fouls_total', 'team']

    missing_cols = [i for i in expected_cols if i not in df.columns]

    if len(missing_cols) != 0:
        raise ValueError(f"CSV is missing the following columns: {[col for col in missing_cols]}")
    
    integer_cols = ['game_id', 'field_goals_made',
                    'field_goals_attempts', 'field_goals_pct', 'threes_made',
                    'threes_attempts', 'threes_pct', 'free_throws_made',
                    'free_throw_attempts', 'free_throw_pct', 'rebounds_total',
                    'rebounds_off', 'rebounds_def', 'assists_total', 'steals_total',
                    'blocks_total', 'turnovers_total', 'personal_fouls_total']

    should_be_int_cols = [col for col in integer_cols if not df[col].dtype == 'int64']

    if len(should_be_int_cols) != 0:
        raise ValueError(f"The following columns should be integers: {[col for col in should_be_int_cols]}")
    
    string_cols = ['home_or_away', 'outcome', 'team']

    should_be_string_cols = [col for col in string_cols if not df[col].dtype == 'object']

    if len(should_be_string_cols) != 0:
        raise ValueError(f"The following columns should be strings: {[col for col in should_be_string_cols]}")
    
    could_not_be_100_cols = ['field_goals_made','field_goals_attempts',
                            'threes_made','threes_attempts', 'free_throws_made',
                            'free_throw_attempts', 'rebounds_total',
                            'rebounds_off', 'rebounds_def', 'assists_total', 'steals_total',
                            'blocks_total', 'turnovers_total', 'personal_fouls_total']
    
    outlier_cols = [col for col in could_not_be_100_cols 
                    if (df[col].values[0] < 0 or df[col].values[0] > 100 or
                        df[col].values[1] < 0 or df[col].values[1] > 100)]

    if len(outlier_cols) != 0:
        raise ValueError(f"The following columns are outliers: {[col for col in outlier_cols]}")