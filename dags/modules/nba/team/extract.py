from datetime import date
from dotenv import load_dotenv
import logging
import os
import requests

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import get_current_context
import pandas as pd

load_dotenv()

# Initialize environment variables

GAME_CSV = os.getenv("WIZARDS_GAME_CSV")
GCS_BUCKET = os.getenv("GCS_BUCKET")
WIZARDS_ID = os.getenv("WIZARDS_ID")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")

# Initialize Google Cloud Platform client 

hook = GCSHook(gcp_conn_id="google_cloud_default")
storage_client = hook.get_conn() 

def ingest_nba():
    context = get_current_context()
    api_date = context['ti'].xcom_pull(key="api_date", task_ids="skip_if_not_release_date")      
    local_date = context['ti'].xcom_pull(key="local_date", task_ids="skip_if_not_release_date")      

    data_to_load = extract(api_date)
    convert_to_csv(data_to_load)
    upload_to_gcs(local_date)

def extract(game_date):

    game_information = [ {"Washington Wizards": {}}, {"opponent": {}} ]

    opponents_name = ""

    # Get game ID and which team won and which team lost
    game_url = f'https://v1.basketball.api-sports.io/games?date={game_date}&team=161&season=2025-2026'
    print(game_url)

    payload = {}
    headers = {
        'x-rapidapi-host': "v1.basketball.api-sports.io",
        'x-apisports-key': f"{API_SPORTS_KEY}"
        }

    response = requests.request("GET", game_url, headers=headers, data=payload)
    
    response = response.json()

    if response['response'][0]['id']:

        if response['response'][0]['teams']['home']['id'] == WIZARDS_ID:
            
            opponents_name = response['response'][0]['teams']['away']['name']
            game_information[1] = {opponents_name: {}}

            game_information[0]["Washington Wizards"]['home_or_away'] = 'home'
            game_information[1][opponents_name]['home_or_away'] = 'away'

        elif response['response'][0]['teams']['home']['id'] != WIZARDS_ID:

            opponents_name = response['response'][0]['teams']['home']['name']
            game_information[1] = {opponents_name: {}}

            game_information[0]["Washington Wizards"]['home_or_away'] = 'away'
            game_information[1][opponents_name]['home_or_away'] = 'home'

        game_id = response['response'][0]['id']
        logging.info("Game id successfully found: ", game_id)

        game_information[0]["Washington Wizards"]['game_id'] = game_id
        game_information[1][opponents_name]['game_id'] = game_id

        if ((response['response'][0]['teams']['home']['id'] == WIZARDS_ID and 
            response['response'][0]['scores']['home']['total'] > response['response'][0]['scores']['away']['total']) or 
            (response['response'][0]['teams']['away']['id'] == WIZARDS_ID and 
            response['response'][0]['scores']['away']['total'] > response['response'][0]['scores']['home']['total'])):
            wizards_outcome = 'win'
            opponent_outcome = 'loss'
        else:
            wizards_outcome = 'loss'
            opponent_outcome = 'win'

        game_information[0]["Washington Wizards"]['outcome'] = wizards_outcome
        game_information[1][opponents_name]['outcome'] = opponent_outcome
        
        logging.info("Outcome of game: Wizards", wizards_outcome)
    else:
        logging.error("No game id found")
        return
    
    # Get stats for both teams from the game
    
    team_stats_url = f'https://v1.basketball.api-sports.io/games/statistics/teams?id={game_id}'

    payload = {}

    response = requests.request("GET", team_stats_url, headers=headers, data=payload)
    response = response.json()

    if response['response'][0]['team']['id'] == WIZARDS_ID:
        
        # Get field goal stats

        game_information[0]["Washington Wizards"]['field_goals_made'] = response['response'][0]['field_goals']['total']
        game_information[0]["Washington Wizards"]['field_goals_attempts'] = response['response'][0]['field_goals']['attempts']
        game_information[0]["Washington Wizards"]['field_goals_pct'] = response['response'][0]['field_goals']['percentage']

        game_information[1][opponents_name]['field_goals_made'] = response['response'][1]['field_goals']['total']
        game_information[1][opponents_name]['field_goals_attempts'] = response['response'][1]['field_goals']['attempts']
        game_information[1][opponents_name]['field_goals_pct'] = response['response'][1]['field_goals']['percentage']

        # Get three point stats

        game_information[0]["Washington Wizards"]['threes_made'] = response['response'][0]['threepoint_goals']['total']
        game_information[0]["Washington Wizards"]['threes_attempts'] = response['response'][0]['threepoint_goals']['attempts']
        game_information[0]["Washington Wizards"]['threes_pct'] = response['response'][0]['threepoint_goals']['percentage']

        game_information[1][opponents_name]['threes_made'] = response['response'][1]['threepoint_goals']['total']
        game_information[1][opponents_name]['threes_attempts'] = response['response'][1]['threepoint_goals']['attempts']
        game_information[1][opponents_name]['threes_pct'] = response['response'][1]['threepoint_goals']['percentage']

        # Get free throw stats

        game_information[0]["Washington Wizards"]['free_throws_made'] = response['response'][0]['freethrows_goals']['total']
        game_information[0]["Washington Wizards"]['free_throw_attempts'] = response['response'][0]['freethrows_goals']['attempts']
        game_information[0]["Washington Wizards"]['free_throw_pct'] = response['response'][0]['freethrows_goals']['percentage']

        game_information[1][opponents_name]['free_throws_made'] = response['response'][1]['freethrows_goals']['total']
        game_information[1][opponents_name]['free_throw_attempts'] = response['response'][1]['freethrows_goals']['attempts']
        game_information[1][opponents_name]['free_throw_pct'] = response['response'][1]['freethrows_goals']['percentage']

        # Get rebound stats

        game_information[0]["Washington Wizards"]['rebounds_total'] = response['response'][0]['rebounds']['total']
        game_information[0]["Washington Wizards"]['rebounds_off'] = response['response'][0]['rebounds']['offence']
        game_information[0]["Washington Wizards"]['rebounds_def'] = response['response'][0]['rebounds']['defense']

        game_information[1][opponents_name]['rebounds_total'] = response['response'][1]['rebounds']['total']
        game_information[1][opponents_name]['rebounds_off'] = response['response'][1]['rebounds']['offence']
        game_information[1][opponents_name]['rebounds_def'] = response['response'][1]['rebounds']['defense']

        # Get assists stats

        game_information[0]["Washington Wizards"]['assists_total'] = response['response'][0]['assists']
        game_information[1][opponents_name]['assists_total'] = response['response'][1]['assists']

        # Get steals stats

        game_information[0]["Washington Wizards"]['steals_total'] = response['response'][0]['steals']
        game_information[1][opponents_name]['steals_total'] = response['response'][1]['steals']

        # Get blocks stats 

        game_information[0]["Washington Wizards"]['blocks_total'] = response['response'][0]['blocks']
        game_information[1][opponents_name]['blocks_total'] = response['response'][1]['blocks']

        # Get turnover stats

        game_information[0]["Washington Wizards"]['turnovers_total'] = response['response'][0]['turnovers']
        game_information[1][opponents_name]['turnovers_total'] = response['response'][1]['turnovers']

        # Get personal foul stats

        game_information[0]["Washington Wizards"]['personal_fouls_total'] = response['response'][0]['personal_fouls']
        game_information[1][opponents_name]['personal_fouls_total'] = response['response'][1]['personal_fouls']

    elif response['response'][1]['team']['id'] != WIZARDS_ID:
        
        # Get field goal stats

        game_information[0]["Washington Wizards"]['field_goals_made'] = response['response'][1]['field_goals']['total']
        game_information[0]["Washington Wizards"]['field_goals_attempts'] = response['response'][1]['field_goals']['attempts']
        game_information[0]["Washington Wizards"]['field_goals_pct'] = response['response'][1]['field_goals']['percentage']

        game_information[1][opponents_name]['field_goals_made'] = response['response'][0]['field_goals']['total']
        game_information[1][opponents_name]['field_goals_attempts'] = response['response'][0]['field_goals']['attempts']
        game_information[1][opponents_name]['field_goals_pct'] = response['response'][0]['field_goals']['percentage']

        # Get three point stats

        game_information[0]["Washington Wizards"]['threes_made'] = response['response'][1]['threepoint_goals']['total']
        game_information[0]["Washington Wizards"]['threes_attempts'] = response['response'][1]['threepoint_goals']['attempts']
        game_information[0]["Washington Wizards"]['threes_pct'] = response['response'][1]['threepoint_goals']['percentage']

        game_information[1][opponents_name]['threes_made'] = response['response'][0]['threepoint_goals']['total']
        game_information[1][opponents_name]['threes_attempts'] = response['response'][0]['threepoint_goals']['attempts']
        game_information[1][opponents_name]['threes_pct'] = response['response'][0]['threepoint_goals']['percentage']

        # Get free throw stats

        game_information[0]["Washington Wizards"]['free_throws_made'] = response['response'][1]['freethrows_goals']['total']
        game_information[0]["Washington Wizards"]['free_throw_attempts'] = response['response'][1]['freethrows_goals']['attempts']
        game_information[0]["Washington Wizards"]['free_throw_pct'] = response['response'][1]['freethrows_goals']['percentage']

        game_information[1][opponents_name]['free_throws_made'] = response['response'][0]['freethrows_goals']['total']
        game_information[1][opponents_name]['free_throw_attempts'] = response['response'][0]['freethrows_goals']['attempts']
        game_information[1][opponents_name]['free_throw_pct'] = response['response'][0]['freethrows_goals']['percentage']

        # Get rebound stats

        game_information[0]["Washington Wizards"]['rebounds_total'] = response['response'][1]['rebounds']['total']
        game_information[0]["Washington Wizards"]['rebounds_off'] = response['response'][1]['rebounds']['offence']
        game_information[0]["Washington Wizards"]['rebounds_def'] = response['response'][1]['rebounds']['defense']

        game_information[1][opponents_name]['rebounds_total'] = response['response'][0]['rebounds']['total']
        game_information[1][opponents_name]['rebounds_off'] = response['response'][0]['rebounds']['offence']
        game_information[1][opponents_name]['rebounds_def'] = response['response'][0]['rebounds']['defense']

        # Get assists stats

        game_information[0]["Washington Wizards"]['assists_total'] = response['response'][1]['assists']
        game_information[1][opponents_name]['assists_total'] = response['response'][0]['assists']

        # Get steals stats

        game_information[0]["Washington Wizards"]['steals_total'] = response['response'][1]['steals']
        game_information[1][opponents_name]['steals_total'] = response['response'][0]['steals']

        # Get blocks stats 

        game_information[0]["Washington Wizards"]['blocks_total'] = response['response'][1]['blocks']
        game_information[1][opponents_name]['blocks_total'] = response['response'][0]['blocks']

        # Get turnover stats

        game_information[0]["Washington Wizards"]['turnovers_total'] = response['response'][1]['turnovers']
        game_information[1][opponents_name]['turnovers_total'] = response['response'][0]['turnovers']

        # Get personal foul stats

        game_information[0]["Washington Wizards"]['personal_fouls_total'] = response['response'][1]['personal_fouls']
        game_information[1][opponents_name]['personal_fouls_total'] = response['response'][0]['personal_fouls']

    return game_information

def convert_to_csv(data):
    flat_data = []

    for d in data:
        for team, stats in d.items():
            stats["team"] = team
            flat_data.append(stats)

    df = pd.DataFrame(flat_data)
    pd.DataFrame.to_csv(df, path_or_buf=f"/tmp/{GAME_CSV}", index=False)

def upload_to_gcs(game_date):

    bucket = storage_client.get_bucket(f"{GCS_BUCKET}")
    object_name_in_gcs_bucket = bucket.blob(f"wizards/game-{game_date}.csv")
    object_name_in_gcs_bucket.upload_from_filename(f"/tmp/{GAME_CSV}")
    logging.info("Sucessfully uploaded: " + f"{GCS_BUCKET}/wizards/game-{game_date}.csv")