from collections import defaultdict
from dotenv import load_dotenv
import logging
import os
import requests
import pandas as pd
import time
import random

load_dotenv()

API_SPORTS_KEY = os.getenv("API_SPORTS_KEY")
WIZARDS_ID = os.getenv("WIZARDS_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GCP_PROJECT = os.getenv("GCP_PROJECT")
WIZARDS_CRAWL_LOG_TABLE = os.getenv("WIZARDS_CRAWL_LOG_TABLE")


class SportsETLHandler:
    """Handles API and validation tasks for NBA data"""

    class NBAAPI:

        @staticmethod
        def setup_and_call(api_url, max_retries=3):

            # Get game ID and which team won and which team lost
            logging.info(api_url)

            payload = {}
            headers = {
                "x-rapidapi-host": "v1.basketball.api-sports.io",
                "x-apisports-key": f"{API_SPORTS_KEY}",
            }

            retry_delay = 1

            for attempt in range(max_retries):
                try:
                    response = requests.request(
                        "GET", api_url, headers=headers, data=payload
                    )
                    return response.json()
                except:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    retry_delay += random.uniform(0, 1)

            raise Exception("Max retries attempted for setup and call function")

        @staticmethod
        def create_extraction_dict_for_game_info(response):

            wizards_dict = defaultdict()
            opponent_dict = defaultdict()
            outcome_dict = defaultdict()

            wizards_dict["team"] = "Washington Wizards"

            if response["response"][0]["id"]:

                game_id = response["response"][0]["id"]
                logging.info("Game id successfully found: ", game_id)

                wizards_dict["game_id"] = game_id
                opponent_dict["game_id"] = game_id
                outcome_dict["game_id"] = game_id

                date = response["parameters"]["date"]

                outcome_dict["game_date"] = date

                logging.info("Home ID:", response["response"][0]["teams"]["home"]["id"])
                logging.info("Away ID:", response["response"][0]["teams"]["away"]["id"])

                # If Wizards are the home team

                if (
                    response["response"][0]["teams"]["home"]["name"]
                    == "Washington Wizards"
                ):

                    logging.info("Wizards are home")

                    wizards_dict["home_or_away"] = "home"

                    opponents_name = response["response"][0]["teams"]["away"]["name"]
                    opponent_dict["team"] = opponents_name
                    logging.info("Opponent's name:", opponents_name)

                elif (
                    response["response"][0]["teams"]["home"]["id"]
                    != "Washington Wizards"
                ):

                    logging.info("Wizards are away")

                    wizards_dict["home_or_away"] = "home"

                    opponents_name = response["response"][0]["teams"]["home"]["name"]
                    opponent_dict["team"] = opponents_name

                if (
                    response["response"][0]["teams"]["home"]["id"] == WIZARDS_ID
                    and response["response"][0]["scores"]["home"]["total"]
                    > response["response"][0]["scores"]["away"]["total"]
                ) or (
                    response["response"][0]["teams"]["away"]["id"] == WIZARDS_ID
                    and response["response"][0]["scores"]["away"]["total"]
                    > response["response"][0]["scores"]["home"]["total"]
                ):
                    wizards_outcome = "win"
                else:
                    wizards_outcome = "loss"

                outcome_dict["outcome"] = wizards_outcome

                logging.info("Outcome of game: Wizards", wizards_outcome)

            else:
                logging.debug("No game id found")
                return

            dict_results = [
                {"Wizards": wizards_dict},
                {"Opponent": opponent_dict},
                {"Outcome": outcome_dict},
            ]

            return game_id, dict_results

        @staticmethod
        def create_extraction_dict_for_game_stats(response, dict_results):

            if response["response"][0]["team"]["id"] == WIZARDS_ID:

                wizards_dict = {
                    "field_goals_made": response["response"][0]["field_goals"]["total"],
                    "field_goals_attempts": response["response"][0]["field_goals"][
                        "attempts"
                    ],
                    "field_goals_pct": response["response"][0]["field_goals"][
                        "percentage"
                    ],
                    "threes_made": response["response"][0]["threepoint_goals"]["total"],
                    "threes_attempts": response["response"][0]["threepoint_goals"][
                        "attempts"
                    ],
                    "threes_pct": response["response"][0]["threepoint_goals"][
                        "percentage"
                    ],
                    "free_throws_made": response["response"][0]["freethrows_goals"][
                        "total"
                    ],
                    "free_throw_attempts": response["response"][0]["freethrows_goals"][
                        "attempts"
                    ],
                    "free_throw_pct": response["response"][0]["freethrows_goals"][
                        "percentage"
                    ],
                    "rebounds_total": response["response"][0]["rebounds"]["total"],
                    "rebounds_off": response["response"][0]["rebounds"]["offence"],
                    "rebounds_def": response["response"][0]["rebounds"]["defense"],
                    "assists_total": response["response"][0]["assists"],
                    "steals_total": response["response"][0]["steals"],
                    "blocks_total": response["response"][0]["blocks"],
                    "turnovers_total": response["response"][0]["turnovers"],
                    "personal_fouls_total": response["response"][0]["personal_fouls"],
                }

                opponents_dict = {
                    "field_goals_made": response["response"][1]["field_goals"]["total"],
                    "field_goals_attempts": response["response"][1]["field_goals"][
                        "attempts"
                    ],
                    "field_goals_pct": response["response"][1]["field_goals"][
                        "percentage"
                    ],
                    "threes_made": response["response"][1]["threepoint_goals"]["total"],
                    "threes_attempts": response["response"][1]["threepoint_goals"][
                        "attempts"
                    ],
                    "threes_pct": response["response"][1]["threepoint_goals"][
                        "percentage"
                    ],
                    "free_throws_made": response["response"][1]["freethrows_goals"][
                        "total"
                    ],
                    "free_throw_attempts": response["response"][1]["freethrows_goals"][
                        "attempts"
                    ],
                    "free_throw_pct": response["response"][1]["freethrows_goals"][
                        "percentage"
                    ],
                    "rebounds_total": response["response"][1]["rebounds"]["total"],
                    "rebounds_off": response["response"][1]["rebounds"]["offence"],
                    "rebounds_def": response["response"][1]["rebounds"]["defense"],
                    "assists_total": response["response"][1]["assists"],
                    "steals_total": response["response"][1]["steals"],
                    "blocks_total": response["response"][1]["blocks"],
                    "turnovers_total": response["response"][1]["turnovers"],
                    "personal_fouls_total": response["response"][1]["personal_fouls"],
                }

            elif response["response"][1]["team"]["id"] != WIZARDS_ID:

                wizards_dict = {
                    "field_goals_made": response["response"][1]["field_goals"]["total"],
                    "field_goals_attempts": response["response"][1]["field_goals"][
                        "attempts"
                    ],
                    "field_goals_pct": response["response"][1]["field_goals"][
                        "percentage"
                    ],
                    "threes_made": response["response"][1]["threepoint_goals"]["total"],
                    "threes_attempts": response["response"][1]["threepoint_goals"][
                        "attempts"
                    ],
                    "threes_pct": response["response"][1]["threepoint_goals"][
                        "percentage"
                    ],
                    "free_throws_made": response["response"][1]["freethrows_goals"][
                        "total"
                    ],
                    "free_throw_attempts": response["response"][1]["freethrows_goals"][
                        "attempts"
                    ],
                    "free_throw_pct": response["response"][1]["freethrows_goals"][
                        "percentage"
                    ],
                    "rebounds_total": response["response"][1]["rebounds"]["total"],
                    "rebounds_off": response["response"][1]["rebounds"]["offence"],
                    "rebounds_def": response["response"][1]["rebounds"]["defense"],
                    "assists_total": response["response"][1]["assists"],
                    "steals_total": response["response"][1]["steals"],
                    "blocks_total": response["response"][1]["blocks"],
                    "turnovers_total": response["response"][1]["turnovers"],
                    "personal_fouls_total": response["response"][1]["personal_fouls"],
                }

                opponents_dict = {
                    "field_goals_made": response["response"][0]["field_goals"]["total"],
                    "field_goals_attempts": response["response"][0]["field_goals"][
                        "attempts"
                    ],
                    "field_goals_pct": response["response"][0]["field_goals"][
                        "percentage"
                    ],
                    "threes_made": response["response"][0]["threepoint_goals"]["total"],
                    "threes_attempts": response["response"][0]["threepoint_goals"][
                        "attempts"
                    ],
                    "threes_pct": response["response"][0]["threepoint_goals"][
                        "percentage"
                    ],
                    "free_throws_made": response["response"][0]["freethrows_goals"][
                        "total"
                    ],
                    "free_throw_attempts": response["response"][0]["freethrows_goals"][
                        "attempts"
                    ],
                    "free_throw_pct": response["response"][0]["freethrows_goals"][
                        "percentage"
                    ],
                    "rebounds_total": response["response"][0]["rebounds"]["total"],
                    "rebounds_off": response["response"][0]["rebounds"]["offence"],
                    "rebounds_def": response["response"][0]["rebounds"]["defense"],
                    "assists_total": response["response"][0]["assists"],
                    "steals_total": response["response"][0]["steals"],
                    "blocks_total": response["response"][0]["blocks"],
                    "turnovers_total": response["response"][0]["turnovers"],
                    "personal_fouls_total": response["response"][0]["personal_fouls"],
                }

            dict_results[0]["Wizards"] = wizards_dict | dict_results[0]["Wizards"]
            dict_results[1]["Opponent"] = opponents_dict | dict_results[1]["Opponent"]

            return dict_results

        @staticmethod
        def upload_game_processing_info_to_bigquery(
            date, bq_client, extracted=False, game_id=None
        ):
            """Upload game_id, date, and whether the date was successfully extracted or not"""
            bq_client.insert_rows_json(
                table=f"{GCP_PROJECT}.{BQ_DATASET}.{WIZARDS_CRAWL_LOG_TABLE}",
                json_rows=[
                    {
                        "game_id": f"{game_id}",
                        "game_date": f"{date}",
                        "extracted": f"{extracted}",
                    }
                ],
            )

    class NBAValidator:
        """Performs tasks relevant to validation of NBA game data"""

        STATS_SCHEMA = {
            "required_columns": [
                "home_or_away",
                "game_id",
                "field_goals_made",
                "field_goals_attempts",
                "field_goals_pct",
                "threes_made",
                "threes_attempts",
                "threes_pct",
                "free_throws_made",
                "free_throw_attempts",
                "free_throw_pct",
                "rebounds_total",
                "rebounds_off",
                "rebounds_def",
                "assists_total",
                "steals_total",
                "blocks_total",
                "turnovers_total",
                "personal_fouls_total",
                "team",
            ],
            "integer_columns": [
                "game_id",
                "field_goals_made",
                "field_goals_attempts",
                "field_goals_pct",
                "threes_made",
                "threes_attempts",
                "threes_pct",
                "free_throws_made",
                "free_throw_attempts",
                "free_throw_pct",
                "rebounds_total",
                "rebounds_off",
                "rebounds_def",
                "assists_total",
                "steals_total",
                "blocks_total",
                "turnovers_total",
                "personal_fouls_total",
            ],
            "string_columns": ["home_or_away", "team"],
            "range_validations": {
                "field_goals_made": (0, 100),
                "threes_made": (0, 100),
                "threes_attempts": (0, 100),
                "free_throws_made": (0, 100),
                "free_throw_attempts": (0, 100),
                "rebounds_total": (0, 100),
                "rebounds_off": (0, 100),
                "rebounds_def": (0, 100),
                "assists_total": (0, 100),
                "steals_total": (0, 100),
                "blocks_total": (0, 100),
                "turnovers_total": (0, 100),
                "personal_fouls_total": (0, 100),
            },
        }

        OUTCOME_SCHEMA = {
            "required_columns": ["game_id", "outcome"],
            "integer_columns": ["game_id"],
            "string_columns": ["outcome"],
        }

        @classmethod
        def validate_stats(cls, df):
            """Validate NBA stats DataFrame"""
            cls.validate_schema(df, cls.STATS_SCHEMA)
            cls.validate_ranges(df, cls.STATS_SCHEMA["range_validations"])

        @classmethod
        def validate_outcome(cls, df):
            """Validate outcome DataFrame"""
            cls.validate_schema(df, cls.OUTCOME_SCHEMA)

        @staticmethod
        def validate_schema(df, schema):

            missing = [
                col for col in schema["required_columns"] if col not in df.columns
            ]

            if missing:
                raise ValueError(f"Missing required columns: {missing}")

            wrong_int_type = [
                col
                for col in schema["integer_columns"]
                if col in df and df[col].dtype != "int64"
            ]

            if wrong_int_type:
                raise ValueError(f"Columns are not integers: {wrong_int_type}")

            wrong_str_type = [
                col
                for col in schema["string_columns"]
                if col in df and df[col].dtype != "object"
            ]

            if wrong_str_type:
                raise ValueError(f"Columns are not strings: {wrong_str_type}")

        @staticmethod
        def validate_ranges(df, schema):

            for col, (min_val, max_val) in schema.items():
                if col in df.columns:
                    invalid = [(df[col] < min_val) | (df[col] > max_val)]
                    if not invalid.empty:
                        raise ValueError(
                            f"Column {col} outside of normal ranges: {min_val, max_val}"
                        )
