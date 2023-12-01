import requests
import numpy as np
import pandas as pd
import os
import json
import json
import re
from sleeper.leagues import SleeperLeaguesAPI
from utils.logger import LoggingConfig
from utils.helpers import DataFrameHelpers
from collections import Counter
from datetime import datetime

# Configure logging
LoggingConfig.configureLog()

# Get a logger for this module
logger = LoggingConfig.getLog(__name__)


class SleeperLeaguesData:
    def __init__(self):
        # Initialize SleeperLeaguesAPI class
        self.sleeper = SleeperLeaguesAPI()

    def getSpecificSleeperLeague(self, league_id):
        league = self.sleeper.getSpecificLeague(league_id)
        return league

    def getSleeperLeagueRosters(self, league_id):
        """
        Get Sleeper league rosters with provided league_id.
        Sleeper league rosters are accessed through the endpoint for retrieving all rosters in a league.
        The endpoint is accessed through a GET HTTP request for `https://api.sleeper.app/v1/league/<league_id>/rosters`.
        From the Sleeper league homepage, the league_id is found in:
        `https://sleeper.com/leagues/league_id/league`

        e.g., roster_info = rosters[0].keys() returns:
        dict_keys(['taxi', 'starters', 'settings', 'roster_id', 'reserve', 'players', 'player_map', 'owner_id', 'metadata', 'league_id', 'keepers', 'co_owners'])

        Args:
            league_id (str): Sleeper League ID

        Returns:
            rosters: List of rosters data for each team.
        """
        # Get league rosters w/ provided league_id
        rosters = self.sleeper.getLeagueRosters(league_id)
        return rosters

    def getSleeperLeagueRostersToDF(self, league_id):
        """
        Get Sleeper league rosters with provided league_id in a Pandas DataFrame.
        Sleeper league rosters are accessed through the endpoint for retrieving all rosters in a league.
        A Pandas DataFrame is created, `df_rosters`, with data stored for each team in individual rows.
        Player data in `df_rosters` is stored as lists for "players", "taxi", "reserve", and "keepers".

        Args:
            league_id (str): Sleeper league ID

        Returns:
            df_rosters: Sleeper league rosters DataFrame.
        """
        # Pull roster data in list from Sleeper endpoint
        rosters = self.getSleeperLeagueRosters(league_id)

        # Record from metadata and settings
        # Extract 'streak' and 'record' from 'metadata' for each roster
        metadata_list = [
            roster.get("metadata", {}).get("streak", "") for roster in rosters
        ]
        record_list = [
            roster.get("metadata", {}).get("record", "") for roster in rosters
        ]
        wins_list = [roster.get("settings", {}).get("wins", "") for roster in rosters]
        losses_list = [
            roster.get("settings", {}).get("losses", "") for roster in rosters
        ]
        ties_list = [roster.get("settings", {}).get("ties", "") for roster in rosters]

        # FAAB Waiver Budget
        waiver_budget_faab = 100
        waiver_budget_used_list = [
            roster.get("settings", {}).get("waiver_budget_used", "")
            for roster in rosters
        ]
        # Calculate the waiver budget remaining for each roster
        waiver_budget_remaining = [
            waiver_budget_faab - used for used in waiver_budget_used_list
        ]

        # Points for, against, max points for
        pf_list = [roster.get("settings", {}).get("fpts", "") for roster in rosters]
        pa_list = [
            roster.get("settings", {}).get("fpts_against", "") for roster in rosters
        ]
        max_pf_list = [roster.get("settings", {}).get("ppts", "") for roster in rosters]

        # Points decimal
        pf_decimal_list = [
            roster.get("settings", {}).get("fpts_decimal", "") for roster in rosters
        ]
        pa_decimal_list = [
            roster.get("settings", {}).get("fpts_against_decimal", "")
            for roster in rosters
        ]
        max_pf_decimal_list = [
            roster.get("settings", {}).get("ppts_decimal", "") for roster in rosters
        ]

        # User ID
        user_id_list = [roster.get("owner_id", "") for roster in rosters]
        # Roster ID
        roster_id_list = [roster.get("roster_id", "") for roster in rosters]
        # League ID
        league_id_list = [roster.get("league_id", "") for roster in rosters]
        # Co-Owners
        co_owners_list = [roster.get("co_owners", "") for roster in rosters]
        # Cleanup so we see co_owners = user_id, not [user_id]
        co_owners_cleaned = [
            item[0] if isinstance(item, list) else item for item in co_owners_list
        ]

        # Players (by ID, not name)
        taxi = [roster.get("taxi", "") for roster in rosters]
        players = [roster.get("players", "") for roster in rosters]
        reserve = [roster.get("reserve", "") for roster in rosters]
        keepers = [roster.get("keepers", "") for roster in rosters]

        # Create a DataFrame with 'streak' and 'record' columns
        df_rosters = pd.DataFrame(
            {
                "user_id": user_id_list,
                "roster_id": roster_id_list,
                "league_id": league_id_list,
                "streak": metadata_list,
                "record": record_list,
                "wins": wins_list,
                "losses": losses_list,
                "ties": ties_list,
                "waiver_budget_used": waiver_budget_used_list,
                "waiver_budget_remaining": waiver_budget_remaining,
                "fpts": pf_list,
                "fpts_against": pa_list,
                "max_fpts": max_pf_list,
                "fpts_decimal": pf_decimal_list,
                "fpts_against_decimal": pa_decimal_list,
                "max_fpts_decimal": max_pf_decimal_list,
                "co_owners": co_owners_cleaned,
                "players": players,
                "reserve": reserve,
                "taxi": taxi,
                "keepers": keepers,
            }
        )
        # Save league rosters DataFrame
        # Returns "players", "taxi", "reserve", "keepers" as lists
        return df_rosters

    def taxiSquadDeadline(self, league_id):
        # Initialize function to get specific league data
        specific_league = self.getSpecificSleeperLeague(league_id)
        taxi_deadline = specific_league.get("settings", {}).get("taxi_deadline", "")

        warning_taxi_deadline = (
            "Once the deadline passes, you may no longer move players into taxi squad."
        )

        if taxi_deadline == 0:
            deadline = "No deadline. Players can go on taxi at anytime."
            logger.info(f"{deadline}")
            taxi_week_deadline = f"None"
        elif taxi_deadline == 1:
            deadline = f"Taxi squad players may be moved until the end of the first week of preseason. {warning_taxi_deadline}"
            logger.info(f"{deadline}")
            taxi_week_deadline = f"Preseason Week {taxi_deadline}"
        elif taxi_deadline == 2:
            deadline = f"Taxi squad players may be moved until the end of the second week of preseason. {warning_taxi_deadline}"
            logger.info(f"{deadline}")
            taxi_week_deadline = f"Preseason Week {taxi_deadline}"
        elif taxi_deadline == 3:
            deadline = f"Taxi squad players may be moved until the end of the third week of preseason. {warning_taxi_deadline}"
            logger.info(f"{deadline}")
            taxi_week_deadline = f"Preseason Week {taxi_deadline}"
        elif taxi_deadline == 4:
            deadline = f"Taxi squad players may be moved until the regular season starts. {warning_taxi_deadline}"
            logger.info(f"{deadline}")
            taxi_week_deadline = f"Start of Regular Season"
        else:
            deadline = f"Taxi squad players deadline not recognized."
            logger.warning(f"{deadline}")
            taxi_week_deadline = f"nan"

        return taxi_week_deadline

    def waiverType(self, league_id):
        # Initialize function to get specific league data
        specific_league = self.getSpecificSleeperLeague(league_id)
        waiver_type = specific_league.get("settings", {}).get("waiver_type", "")

        if waiver_type == 0:
            type = "Rolling Waivers"
            waiver = f"Waiver Type: {type}"
            logger.info(f"{waiver}")
        elif waiver_type == 1:
            type = "Reverse Standings"
            deadline = f"Waiver Type: {type}"
            logger.info(f"{waiver}")
        elif waiver_type == 2:
            type = "FAAB Bidding"
            waiver = f"Waiver Type: {type}"
            waiver_budget = specific_league.get("settings", {}).get("waiver_budget", "")
            logger.info(f"{waiver} (Budget: ${waiver_budget})")
        else:
            type = "None"
            waiver = f"Waiver type not recognized."
            logger.warning(f"{waiver}")

        return type

    def playoffSeedType(self, league_id):
        # Initialize function to get specific league data
        specific_league = self.getSpecificSleeperLeague(league_id)
        playoff_seed_type = specific_league.get("settings", {}).get(
            "playoff_seed_type", ""
        )

        if playoff_seed_type == 0:
            type = "Default"
            rule = f"({type}) Teams stay on their initial side of the bracket throughout the playoffs."
            logger.warning(f"{rule}")
        if playoff_seed_type == 1:
            type = "Re-seed"
            rule = f"({type}) Higher seeded teams always play lower-seeded teams in every round."
            logger.warning(f"{rule}")

        return type

    def getSpecificLeagueDF(self, league_id):
        # Initialize function to get specific league data
        specific_league = self.getSpecificSleeperLeague(league_id)
        roster_positions = specific_league["roster_positions"]
        # Count the occurrences of each position
        position_counts = Counter(roster_positions)

        # Create a string representation to count and list all pos in str
        positions = ",".join(
            f"{count}{position}" if count > 1 else f"{count}{position}"
            for position, count in position_counts.items()
        )
        scoring_settings = specific_league["scoring_settings"]
        settings = specific_league["settings"]
        metadata = specific_league["metadata"]
        # Rosters
        teams = specific_league.get("settings", {}).get("num_teams", "")
        reserve_slots = specific_league.get("settings", {}).get("reserve_slots", "")
        reserve_covid_slots = specific_league.get("settings", {}).get(
            "reserve_allow_cov", ""
        )
        reserve_allow_out = specific_league.get("settings", {}).get(
            "reserve_allow_out", ""
        )
        taxi_slots = specific_league.get("settings", {}).get("taxi_slots", "")
        taxi_years = specific_league.get("settings", {}).get("taxi_years", "")
        taxi_deadline = self.taxiSquadDeadline(league_id)

        # Waivers
        # CHANGE TO FUNCTION
        waiver_type = self.waiverType(league_id)
        waiver_budget = specific_league.get("settings", {}).get("waiver_budget", "")
        # 0 (Monday) 1 (Tuesday) 2 (Wednesday)
        waiver_day_of_week = specific_league.get("settings", {}).get(
            "waiver_day_of_week", ""
        )
        # Waivers hours are waiver_day_of_week and daily after in PST
        daily_waivers_hour = specific_league.get("settings", {}).get(
            "daily_waivers_hour", ""
        )
        # Amount of days for waiver to clear
        waiver_clear_days = specific_league.get("settings", {}).get(
            "waiver_clear_days", ""
        )
        # Last waivers ran date
        waiver_date = specific_league.get("settings", {}).get(
            "daily_waivers_last_ran", ""
        )
        if waiver_date is not None:
            timestamp = datetime.now()
            # Format timestamp with current year, month, and waiver_date
            formatted_timestamp = timestamp.strftime(f"%Y-%b-{waiver_date}")
        else:
            formatted_timestamp = ""

        # Deadlines
        start_week = specific_league.get("settings", {}).get("start_week", "")
        # CHANGE TO FUNCTION
        trade_deadline = specific_league.get("settings", {}).get("trade_deadline", "")

        # Playoffs
        playoff_week_start = specific_league.get("settings", {}).get(
            "playoff_week_start", ""
        )
        playoff_teams = specific_league.get("settings", {}).get("playoff_teams", "")
        # Playoff seed type
        playoff_seed_type = self.playoffSeedType(league_id)

        # Dynasty
        draft_rounds = specific_league.get("settings", {}).get("draft_rounds", "")

        # League Information
        league_id = specific_league["league_id"]
        previous_league_id = specific_league["previous_league_id"]
        draft_id = specific_league["draft_id"]
        loser_bracket_id = specific_league["loser_bracket_id"]
        bracket_id = specific_league["bracket_id"]
        league_name = specific_league["name"]
        league_dict = [
            {
                "league_name": league_name,
                "league_id": league_id,
                "previous_league_id": previous_league_id,
                "draft_id": draft_id,
                "bracket_id": bracket_id,
                "loser_bracket_id": loser_bracket_id,
                "teams": teams,
                "reserve_slots": reserve_slots,
                "reserve_covid_slots": reserve_covid_slots,
                "reserve_allow_out": reserve_allow_out,
                "taxi_slots": taxi_slots,
                "taxi_years": taxi_years,
                "taxi_deadline": taxi_deadline,
                "waiver_type": waiver_type,
                "waiver_budget": waiver_budget,
                "waiver_day_of_week": waiver_day_of_week,
                "daily_waivers_hour_PST": daily_waivers_hour,
                "waiver_clear_days": waiver_clear_days,
                "daily_waivers_last_ran": formatted_timestamp,
                "start_week": start_week,
                "trade_deadline": trade_deadline,
                "playoff_week_start": playoff_week_start,
                "playoff_teams": playoff_teams,
                "playoff_seed_type": playoff_seed_type,
                "draft_rounds": draft_rounds,
                "roster_positions": positions,
            }
        ]
        df_league = pd.DataFrame(league_dict)
        return df_league

    def getSpecificLeaguePos(self, league_id):
        # Initialize function to get specific league data
        specific_league = self.getSpecificSleeperLeague(league_id)
        # Nested list with roster positions
        positions = specific_league["roster_positions"]
        taxi_slots = specific_league.get("settings", {}).get("taxi_slots", "")
        reserve_slots = specific_league.get("settings", {}).get("reserve_slots", "")

        # Count the occurrences of each position
        position_counts = Counter(positions)

        # Create a dictionary with columns and counts
        roster_dict = {
            "QB": position_counts["QB"],
            "RB": position_counts["RB"],
            "WR": position_counts["WR"],
            "TE": position_counts["TE"],
            "FLEX": position_counts["FLEX"],
            "SUPER_FLEX": position_counts["SUPER_FLEX"],
            "BN": position_counts["BN"],
            "TAXI": taxi_slots,
            "IR": reserve_slots,
        }

        # Create a string of all pos e.g., 1QB,2RB,3WR,1TE,2FLEX,1SUPER_FLEX,10BN
        pos_string = ",".join(
            f"{count}{position}" if count > 1 else f"{count}{position}"
            for position, count in position_counts.items()
        )

        # Create a Pandas DataFrame
        roster_pos_df = pd.DataFrame([roster_dict])
        return roster_pos_df

    def getSpecificLeagueScoring(self, league_id):
        # TODO: Add descriptions of each setting
        # Initialize function to get specific league data
        specific_league = self.getSpecificSleeperLeague(league_id)
        # Nested list with scoring settings
        scoring_settings = specific_league["scoring_settings"]

        # Create a dataframe and transpose
        # flattens json structure to df
        df_scoring = pd.json_normalize(scoring_settings).T
        df_scoring.reset_index(inplace=True)
        # Rename the columns
        df_scoring.columns = ["setting", "points"]

        return df_scoring

    def getSleeperLeagueUsers(self, league_id):
        # Initialize SleeperLeague class
        sleeper = SleeperLeaguesAPI()
        # Fetch league user data from sleeper endpoint
        users = sleeper.getLeagueUsers(league_id)
        # flattens json structure to df
        users_df = pd.json_normalize(users)
        return users_df

    def getSleeperLeagueRosterData(self, league_id):
        # Rosters df
        rosters = self.getSleeperLeagueRostersToDF(league_id)
        # Get league users df
        users = self.getSleeperLeagueUsers(league_id)
        # Merge DataFrames on 'user_id'
        merged_df = pd.merge(rosters, users, on="user_id", how="left")

        # Apply the condition to fill NaN values in metadata.team_name
        # If team_name is NaN, rename to Team <display_name>
        merged_df["metadata.team_name"] = merged_df["metadata.team_name"].fillna(
            merged_df["display_name"].apply(lambda x: f"Team {x}")
        )
        # Get all columns starts with metadata.mascot_item_type_id
        mascot_id_columns = [
            col
            for col in merged_df.columns
            if col.startswith("metadata.mascot_item_type_id")
        ]
        # Get all columns starts with metadata.mascot_message_emotion
        mascot_emotion_columns = [
            col
            for col in merged_df.columns
            if col.startswith("metadata.mascot_message_emotion")
        ]
        # Additional metadata columns
        metadata_cols = [
            "metadata.mention_pn",
            "metadata.archived",
            "metadata.allow_sms",
            "metadata.allow_pn",
            "metadata.show_mascots",
        ]
        # Combine both lists
        columns_to_drop = mascot_id_columns + mascot_emotion_columns + metadata_cols
        # Drop mascot metadata columns
        merged_df = merged_df.drop(columns=columns_to_drop)
        # When using pd.merge "on" for columns, they split with suffixes _x or _y
        # To cleanup the df remove the suffixes
        parsed_df = DataFrameHelpers.removeSplitColNameSuffix(
            merged_df, suffixes=["_y", "_x"]
        )

        return parsed_df

    def getSleeperLeagueMatchups(self, league_id, week):
        # Fetch matchup data
        league = SleeperLeaguesAPI()
        wk1 = league.getMatchupsWeekly(league_id, week=week)
        players_points = [matchup.get("players_points", "") for matchup in wk1]
        # Use list comprehension to extract keys and values for each 'players_points'
        keys_list = [list(matchup["players_points"].keys()) for matchup in wk1]
        values_list = [list(matchup["players_points"].values()) for matchup in wk1]

        starters_points = [matchup.get("starters_points", "") for matchup in wk1]
        starters = [matchup.get("starters", "") for matchup in wk1]
        matchup_id = [matchup.get("matchup_id", "") for matchup in wk1]
        roster_id = [matchup.get("roster_id", "") for matchup in wk1]
        players = [matchup.get("players", "") for matchup in wk1]
        points = [matchup.get("points", "") for matchup in wk1]

        data = {
            "starters_points": starters_points,
            "starters": starters,
            "matchup_id": matchup_id,
            "roster_id": roster_id,
            "players": keys_list,
            "points": values_list,
            "fpts": points,
            "week": week,
        }
        df = pd.DataFrame(data)
        return df

    def getRecursivePreviousLeagueDF(self, league_id, season=2023):
        current_league_df = self.getSpecificLeagueDF(league_id)

        # Set the season for the current league DataFrame
        current_league_df["season"] = season

        if current_league_df.empty:
            # If the DataFrame is empty, return an empty DataFrame
            return current_league_df

        previous_league_id = current_league_df["previous_league_id"].iloc[0]

        if pd.isnull(previous_league_id):
            # If there is no previous league ID, return the current league DataFrame
            return current_league_df
        else:
            # Recursively call the function with the previous league ID and the updated season
            previous_league_df = self.getRecursivePreviousLeagueDF(
                previous_league_id, season - 1
            )

            # Concatenate the current league DataFrame with the previous league DataFrame
            # Set the column order explicitly
            final_df = pd.concat([current_league_df, previous_league_df], axis=0)[
                current_league_df.columns
            ]

            return final_df

    def getSleeperLeagueHistoricalData(self, league_id, season=2023):
        """
        Get Sleeper league historical data such as previous league_id and bracket_id.

        Reference: https://docs.sleeper.com/#getting-the-playoff-bracket
        `r` (int): The round for this matchup, 1st, 2nd, 3rd round, etc.
        `m` (int): The match `id` of the matchup, unique for all matchups within a bracket.
        `t1` (int): The `roster_id` of a team in this matchup OR `{w: 1}` which means the winner of match id `1`
        `t2` (int): The `roster_id` of the other team in this matchup OR `{l: 1}` which means the loser of match id `1`
        `w` (int): The `roster_id` of the winning team, if the match has been played.
        `l` (int): The `roster_id` of the losing team, if the match has been played.

                Args:
                    league_id (_type_): _description_
                    season (int, optional): _description_. Defaults to 2023.

                Returns:
                    _type_: _description_
        """
        # Get dataframe of league_id
        current_league_df = self.getSpecificLeagueDF(league_id)

        # Set the season for the current league DataFrame
        current_league_df["season"] = season

        previous_league_id = current_league_df["previous_league_id"].iloc[0]

        if pd.isnull(previous_league_id):
            # If there is no previous league ID, return the current league DataFrame
            return current_league_df
        else:
            # Recursively call the function with the previous league ID and the updated season
            previous_league_df = self.getRecursivePreviousLeagueDF(
                previous_league_id, season - 1
            )

            # Move the "season" column to the first position
            previous_league_df = pd.concat(
                [
                    previous_league_df["season"],
                    previous_league_df.drop(columns=["season"]),
                ],
                axis=1,
            )

            # Concatenate the current league DataFrame with the previous league DataFrame
            league_history_df = pd.concat(
                [current_league_df, previous_league_df], axis=0
            )
            # 'season' is the column you want to move to the first position
            column_to_move = "season"
            # Get the list of columns excluding the one to move
            columns = [column_to_move] + [
                col for col in league_history_df.columns if col != column_to_move
            ]
            # Reorder the DataFrame with the new column order
            league_history_df = league_history_df[columns]

            return league_history_df

    def getPrevLeaguePlayoffBracket(self, league_id, league_id_season=2023):
        league_history = self.getSleeperLeagueHistoricalData(
            league_id, season=league_id_season
        )
        # Looking for the previous year
        previous_season = league_id_season - 1
        # Get the previous season data
        previous_season_data = league_history[
            league_history["season"] == previous_season
        ]

        if previous_season_data.empty or pd.isnull(
            previous_season_data["previous_league_id"].iloc[0]
        ):
            # Handle the case where there is no data for the previous season or previous_league_id is None
            # You can return a default value, raise an exception, or handle it as needed
            return None  # Change this line based on your desired behavior
        else:
            previous_league_id = previous_season_data["previous_league_id"].iloc[0]
            playoff_bracket = SleeperLeaguesAPI().getPlayoffBracket(
                previous_league_id, "Winners"
            )
            return playoff_bracket

    def getPrevLeaguePlayoffBracketDF(self, league_id, league_id_season=2023):
        playoff_bracket = self.getPrevLeaguePlayoffBracket(
            league_id, league_id_season=league_id_season
        )

        if playoff_bracket is None:
            print("No playoff bracket data found.")
            return pd.DataFrame(
                {
                    "matchup_round": [None],
                    "match_id": [None],
                    "winning_team": [None],
                    "losing_team": [None],
                    "team_1": [None],
                    "team_2": [None],
                    "matchup_desc": [None],
                }
            )

        playoffs_dict = []

        for round in range(len(playoff_bracket)):
            bracket = playoff_bracket[round]
            team_1 = bracket.get("t1")
            team_2 = bracket.get("t2")
            winning_team = bracket.get("w")
            losing_team = bracket.get("l")
            matchup_round = bracket.get("r")
            match_id = bracket.get("m")

            if "t1_from" in bracket and "w" in bracket["t1_from"]:
                matchup_desc = (
                    "Championship: Winner of Match "
                    + str(bracket["t1_from"]["w"])
                    + "/"
                    + str(bracket["t2_from"]["w"])
                )
            elif "t1_from" in bracket and "l" in bracket["t1_from"]:
                matchup_desc = (
                    "Battle for 3rd: Losers of Match "
                    + str(bracket["t1_from"]["l"])
                    + "/"
                    + str(bracket["t2_from"]["l"])
                )
            elif "p" in bracket and "t1_from" not in bracket:
                matchup_desc = f"Battle for 5th (Rd {str(matchup_round)}): Teams {str(team_1)}/{str(team_2)}"
            else:
                matchup_str = f": Teams {str(team_1)}/{str(team_2)}"
                matchup_desc = "Rd " + str(matchup_round) + matchup_str

            playoffs_dict.append(
                {
                    "matchup_round": matchup_round,
                    "match_id": match_id,
                    "winning_team": winning_team,
                    "losing_team": losing_team,
                    "team_1": team_1,
                    "team_2": team_2,
                    "matchup_desc": matchup_desc,
                }
            )

        df = pd.DataFrame(playoffs_dict)
        return df

    def getDraftPickTrades(self, league_id):
        # Initialize request
        sleeper = SleeperLeaguesAPI()
        # retrieve league traded picks from sleeper api endpoint
        traded_picks = sleeper.getTradedPicks(league_id)
        # flattens json structure to df
        picks_df = pd.json_normalize(traded_picks)
        return picks_df

    def getCurrentStateNFL(self):
        sleeper = SleeperLeaguesAPI()
        # retrieve from api endpoint
        state = sleeper.getStateNFL()
        # flattens json structure to df
        state_df = pd.json_normalize(state)
        return state_df

    def getWeeklyTransactions(self, league_id, week):
        # Initialize sleeper api endpoint
        sleeper = SleeperLeaguesAPI()
        trans = sleeper.getTransactionsWeekly(league_id, week=week)
        df = pd.json_normalize(trans)
        rows, columns = df.shape
        # Initialize empty lists to store player_id_add and roster_id_add
        player_id_add_list = []
        roster_id_add_list = []

        for adds in range(rows):
            try:
                player_id_add = list(trans[adds]["adds"].keys())[0]
                roster_id_add = list(trans[adds]["adds"].values())[0]
            except AttributeError:
                player_id_add = "None"
                roster_id_add = "None"

            # Append values to the lists
            player_id_add_list.append(player_id_add)
            roster_id_add_list.append(roster_id_add)

        # Add the processed lists as new columns to the DataFrame
        df["player_id_add"] = player_id_add_list
        df["roster_id_add"] = roster_id_add_list

        # Columns that start with "" into lists
        drops_columns = [col for col in df.columns if col.startswith("drops.")]
        adds_columns = [col for col in df.columns if col.startswith("adds.")]
        # Combine both lists
        columns_to_drop = drops_columns + adds_columns
        # Drop add/drop cols
        df = df.drop(columns=columns_to_drop)
        df.keys()
        rows, columns = df.shape

        transactions_dict = []
        for trade in range(rows):
            transaction_type = df["type"][trade]
            status_updated = df["status_updated"][trade]
            status = df["status"][trade]
            notes = df["metadata.notes"][trade]
            transaction_id = df["transaction_id"][trade]
            creator = df["creator"][trade]
            created = df["created"][trade]
            player_id_add = df["player_id_add"][trade]
            roster_id_add = df["roster_id_add"][trade]
            try:
                pick = df["draft_picks"][trade][0]
                # draft picks
                previous_owner_id = pick["previous_owner_id"]
                owner_id = pick["owner_id"]
                roster_id = pick["roster_id"]
                league_id = pick["league_id"]
                season = pick["season"]
                pick_round = pick["round"]
                print(pick_round)

                waiver_budget = df["waiver_budget"][trade][0]
                sender = str(waiver_budget["sender"])
                receiver = str(waiver_budget["receiver"])
                amount = str(waiver_budget["amount"])
            except IndexError:
                sender = np.nan
                receiver = np.nan
                amount = np.nan
                previous_owner_id = np.nan
                owner_id = np.nan
                roster_id = np.nan
                league_id = np.nan
                season = np.nan
                pick_round = np.nan

            transactions_dict.append(
                {
                    "week": week,
                    "type": transaction_type,
                    "status_updated": status_updated,
                    "status": status,
                    "notes": notes,
                    "transaction_id": transaction_id,
                    "creator": creator,
                    "created": created,
                    "player_id_add": player_id_add,
                    "roster_id_add": roster_id_add,
                    "previous_owner_id": previous_owner_id,
                    "owner_id": owner_id,
                    "roster_id": roster_id,
                    "league_id": league_id,
                    "pick_season": season,
                    "pick_round": pick_round,
                    "sender_id_faab": sender,
                    "receiver_id_faab": receiver,
                    "amount_faab": amount,
                }
            )
        week_transactions = pd.DataFrame(transactions_dict)
        return week_transactions
