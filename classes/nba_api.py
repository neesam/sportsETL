from collections import defaultdict
from dotenv import load_dotenv
import logging
import os
import requests

load_dotenv()

API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")
WIZARDS_ID = os.getenv("WIZARDS_ID")

class NBA_API:
    
    def setup_and_call(api_url):

        # Get game ID and which team won and which team lost
        logging.info(api_url)

        payload = {}
        headers = {
            'x-rapidapi-host': "v1.basketball.api-sports.io",
            'x-apisports-key': f"{API_SPORTS_KEY}"
            }

        response = requests.request("GET", api_url, headers=headers, data=payload)

        return response.json()

    def create_extraction_dict_for_basic_game_info(response):

        wizards_dict = defaultdict()
        opponent_dict = defaultdict()

        outcome_dict = defaultdict()

        wizards_dict['team'] = 'Washington Wizards'
        
        if response['response'][0]['id']:

            wizards_dict['game_id'] = game_id
            opponent_dict['game_id'] = game_id
            outcome_dict['game_id'] = game_id

            logging.info("Home ID:", response['response'][0]['teams']['home']['id'])
            logging.info("Away ID:", response['response'][0]['teams']['away']['id'])

            # If Wizards are the home team

            if response['response'][0]['teams']['home']['name'] == 'Washington Wizards':

                logging.info("Wizards are home")

                wizards_dict['home_or_away'] = 'home'
                opponent_dict['home_or_away'] = 'away'

                opponents_name = response['response'][0]['teams']['away']['name']
                opponent_dict['team'] = opponents_name
                logging.info("Opponent's name:", opponents_name)

            elif response['response'][0]['teams']['home']['id'] != 'Washington Wizards':

                logging.info("Wizards are away")

                wizards_dict['home_or_away'] = 'home'
                opponent_dict['home_or_away'] = 'away'  

                opponents_name = response['response'][0]['teams']['home']['name']
                opponent_dict['team'] = opponents_name

            game_id = response['response'][0]['id']
            logging.info("Game id successfully found: ", game_id)

            if ((response['response'][0]['teams']['home']['id'] == WIZARDS_ID and 
                response['response'][0]['scores']['home']['total'] > response['response'][0]['scores']['away']['total']) or 
                (response['response'][0]['teams']['away']['id'] == WIZARDS_ID and 
                response['response'][0]['scores']['away']['total'] > response['response'][0]['scores']['home']['total'])):
                wizards_outcome = 'win'
            else:
                wizards_outcome = 'loss'

            outcome_dict['outcome'] = wizards_outcome
            
            logging.info("Outcome of game: Wizards", wizards_outcome)

        else:
            logging.error("No game id found")
            return
        
        dict_results = [{'Wizards': wizards_dict}, \
                    {'Opponent': opponent_dict}, \
                    {'Outcome': outcome_dict}
                    ]
        
        return game_id, dict_results
    
    def create_extraction_dict_for_game_stats(response, dict_results):
        
        if response['response'][0]['team']['id'] == WIZARDS_ID:

            wizards_dict = {
                'field_goals_made': response['response'][0]['field_goals']['total'],
                'field_goals_attempts': response['response'][0]['field_goals']['attempts'],
                'field_goals_pct': response['response'][0]['field_goals']['percentage'],
                'threes_made': response['response'][0]['threepoint_goals']['total'],
                'threes_attempts': response['response'][0]['threepoint_goals']['attempts'],
                'threes_pct': response['response'][0]['threepoint_goals']['percentage'],
                'free_throws_made': response['response'][0]['freethrows_goals']['total'],
                'free_throw_attempts': response['response'][0]['freethrows_goals']['attempts'],
                'free_throw_pct': response['response'][0]['freethrows_goals']['percentage'],
                'rebounds_total': response['response'][0]['rebounds']['total'],
                'rebounds_off': response['response'][0]['rebounds']['offence'],
                'rebounds_def': response['response'][0]['rebounds']['defense'],
                'assists_total': response['response'][0]['assists'],
                'steals_total': response['response'][0]['steals'],
                'blocks_total': response['response'][0]['blocks'],
                'turnovers_total': response['response'][0]['turnovers'],
                'personal_fouls_total': response['response'][0]['personal_fouls']
            }

            opponents_dict = {
                'field_goals_made': response['response'][1]['field_goals']['total'],
                'field_goals_attempts': response['response'][1]['field_goals']['attempts'],
                'field_goals_pct': response['response'][1]['field_goals']['percentage'],
                'threes_made': response['response'][1]['threepoint_goals']['total'],
                'threes_attempts': response['response'][1]['threepoint_goals']['attempts'],
                'threes_pct': response['response'][1]['threepoint_goals']['percentage'],
                'free_throws_made': response['response'][1]['freethrows_goals']['total'],
                'free_throw_attempts': response['response'][1]['freethrows_goals']['attempts'],
                'free_throw_pct': response['response'][1]['freethrows_goals']['percentage'],
                'rebounds_total': response['response'][1]['rebounds']['total'],
                'rebounds_off': response['response'][1]['rebounds']['offence'],
                'rebounds_def': response['response'][1]['rebounds']['defense'],
                'assists_total': response['response'][1]['assists'],
                'steals_total': response['response'][1]['steals'],
                'blocks_total': response['response'][1]['blocks'],
                'turnovers_total': response['response'][1]['turnovers'],
                'personal_fouls_total': response['response'][1]['personal_fouls']
            }

        elif response['response'][1]['team']['id'] != WIZARDS_ID:

            wizards_dict = {
                'field_goals_made': response['response'][1]['field_goals']['total'],
                'field_goals_attempts': response['response'][1]['field_goals']['attempts'],
                'field_goals_pct': response['response'][1]['field_goals']['percentage'],
                'threes_made': response['response'][1]['threepoint_goals']['total'],
                'threes_attempts': response['response'][1]['threepoint_goals']['attempts'],
                'threes_pct': response['response'][1]['threepoint_goals']['percentage'],
                'free_throws_made': response['response'][1]['freethrows_goals']['total'],
                'free_throw_attempts': response['response'][1]['freethrows_goals']['attempts'],
                'free_throw_pct': response['response'][1]['freethrows_goals']['percentage'],
                'rebounds_total': response['response'][1]['rebounds']['total'],
                'rebounds_off': response['response'][1]['rebounds']['offence'],
                'rebounds_def': response['response'][1]['rebounds']['defense'],
                'assists_total': response['response'][1]['assists'],
                'steals_total': response['response'][1]['steals'],
                'blocks_total': response['response'][1]['blocks'],
                'turnovers_total': response['response'][1]['turnovers'],
                'personal_fouls_total': response['response'][1]['personal_fouls']
            }

            opponents_dict = {
                'field_goals_made': response['response'][0]['field_goals']['total'],
                'field_goals_attempts': response['response'][0]['field_goals']['attempts'],
                'field_goals_pct': response['response'][0]['field_goals']['percentage'],
                'threes_made': response['response'][0]['threepoint_goals']['total'],
                'threes_attempts': response['response'][0]['threepoint_goals']['attempts'],
                'threes_pct': response['response'][0]['threepoint_goals']['percentage'],
                'free_throws_made': response['response'][0]['freethrows_goals']['total'],
                'free_throw_attempts': response['response'][0]['freethrows_goals']['attempts'],
                'free_throw_pct': response['response'][0]['freethrows_goals']['percentage'],
                'rebounds_total': response['response'][0]['rebounds']['total'],
                'rebounds_off': response['response'][0]['rebounds']['offence'],
                'rebounds_def': response['response'][0]['rebounds']['defense'],
                'assists_total': response['response'][0]['assists'],
                'steals_total': response['response'][0]['steals'],
                'blocks_total': response['response'][0]['blocks'],
                'turnovers_total': response['response'][0]['turnovers'],
                'personal_fouls_total': response['response'][0]['personal_fouls']
            }

        dict_results[0]['Wizards'] = wizards_dict | dict_results[0]['Wizards']
        dict_results[1]['Opponent'] = opponents_dict | dict_results[1]['Opponent']

        return dict_results