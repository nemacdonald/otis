from sleeper.request import RequestAPI, APIException
from utils.logger import LoggingConfig
import requests
import json
from datetime import datetime

# Configure logging
LoggingConfig.configureLog()

# Get a logger for this module
logger = LoggingConfig.getLog(__name__)


class SleeperLeaguesAPI(RequestAPI):
    def __init__(self):
        # Allow using _call from RequestAPI
        super().__init__()

    def getSpecificLeague(self, league_id):
        """
        Retrieve information about a Sleeper league.

        Args:
            league_id (str): The ID of the Sleeper league.

        Returns:
            dict: Information about the specified league.
        """
        try:
            url = f"https://api.sleeper.app/v1/league/{league_id}"
            league = self._call(url)
            logger.info(f"Successfully fetched league data for league ID {league_id}")
            return league
        except APIException as e:
            logger.error(f"Error fetching league data for league ID {league_id}: {e}")
            raise

    def getLeagueRosters(self, league_id, save_to_file: str = None):
        """
        _summary_

        Args:
            league_id (_type_): _description_
            save_to_file (str, optional): File name to save json file with timestamp. Defaults to None.

        Returns:
            rosters: Dictionary of rosters for Sleeper league
        """
        try:
            url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
            rosters = self._call(url)

            if save_to_file:
                # Timestamp for filename prefix
                timestamp = datetime.now().strftime("%Y%m%d")

                # Save to json file
                filename = f"{save_to_file}_{timestamp}.json"
                with open(filename, "w") as file:
                    json.dump(rosters, file, indent=2)

            logger.info(f"Successfully fetched rosters for league ID {league_id}")
            return rosters
        except APIException as e:
            logger.error(f"Error fetching rosters for league ID {league_id}: {e}")
            raise

    def getLeagueUsers(self, league_id):
        """
        Retrieve information about users in a Sleeper league.

        Args:
            league_id (str): The ID of the Sleeper league.

        Returns:
            dict: Information about users in the specified league.
        """
        try:
            url = f"https://api.sleeper.app/v1/league/{league_id}/users"
            users = self._call(url)
            return users
        except APIException as e:
            logger.error(f"Error fetching league users for league ID {league_id}: {e}")
            raise

    def getMatchupsWeekly(self, league_id, week):
        """
        Retrieve matchup data for a specific week in a Sleeper league.

        Args:
            league_id (str): The ID of the Sleeper league.
            week (int): The week for which matchup data is requested.

        Returns:
            dict: Matchup data for the specified week.
        """
        try:
            # Validate if the provided week is within a valid range (e.g., 1 to 17 for an NFL regular season)
            if not (1 <= week <= 17):
                raise ValueError(
                    "Invalid week. Week must be 1-14 for regular season and 15-17 for playoffs."
                )

            url = f"https://api.sleeper.app/v1/league/{league_id}/matchups/{week}"
            matchups = self._call(url)
            return matchups
        except APIException as e:
            msg = f"Error fetching Week {week} matchup data for league ID {league_id}: {e}"
            logger.error(msg)
            raise

    def getPlayoffBracket(self, league_id, bracket="Winners"):
        """
        Retrieve playoff bracket data for a specified league.

        Args:
            league_id (str): The ID of the Sleeper league.
            bracket (str, optional): The type of bracket ('Winners' or 'Losers'). Defaults to 'Winners'.

        Returns:
            dict: Playoff bracket data.
        """
        try:
            # Lowercase str to be case insensitive
            bracket_lower = bracket.lower()

            if bracket_lower not in ["winners", "losers"]:
                raise ValueError("Invalid bracket type. Choose 'Winners' or 'Losers'.")

            url = (
                f"https://api.sleeper.app/v1/league/{league_id}/{bracket_lower}_bracket"
            )
            bracket_data = self._call(url)

            logger.info(
                f"Successfully fetched {bracket_lower} playoff bracket for league ID {league_id}"
            )
            return bracket_data
        except APIException as e:
            logger.error(
                f"Error fetching {bracket_lower} playoff bracket for league ID {league_id}: {e}"
            )
            raise
        except ValueError as ve:
            logger.error(
                f"Invalid bracket type specified for league ID {league_id}: {ve}"
            )
            raise

    def getTransactionsWeekly(self, league_id, week):
        """
        Retrieve transactions data for a specific week in a Sleeper league.

        Args:
            league_id (str): The ID of the Sleeper league.
            week (int): The week for which transactions data is requested.

        Returns:
            dict: Transactions data for the specified week.
        """
        try:
            url = f"https://api.sleeper.app/v1/league/{league_id}/transactions/{week}"
            transactions = self._call(url)

            logger.info(
                f"Successfully fetched transactions for week {week} in league ID {league_id}"
            )
            return transactions
        except APIException as e:
            logger.error(
                f"Error fetching transactions for week {week} in league ID {league_id}: {e}"
            )
            raise

    def getTradedPicks(self, league_id):
        """
        Retrieve information about traded picks in a Sleeper league, including future picks.

        Args:
            league_id (str): The ID of the Sleeper league.

        Returns:
            dict: Traded picks information.
        """
        try:
            url = f"https://api.sleeper.app/v1/league/{league_id}/traded_picks"
            picks = self._call(url)

            logger.info(
                f"Successfully fetched traded picks information for league ID {league_id}"
            )
            return picks
        except APIException as e:
            logger.error(
                f"Error fetching traded picks information for league ID {league_id}: {e}"
            )
            raise

    def getStateNFL(self):
        """
        Retrieve the current state of the NFL.

        Returns:
            dict: Current state of the NFL.
        """
        try:
            url = "https://api.sleeper.app/v1/state/nfl"
            state = self._call(url)

            logger.info("Successfully fetched current state of the NFL")
            return state
        except APIException as e:
            logger.error(f"Error fetching current state of the NFL: {e}")
            raise
