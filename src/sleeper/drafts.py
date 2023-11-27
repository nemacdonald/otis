from sleeper.request import RequestAPI, APIException
from utils.logger import LoggingConfig
import requests
import json
from datetime import datetime

# Configure logging
LoggingConfig.configureLog()

# Get a logger for this module
logger = LoggingConfig.getLog(__name__)

# Get all drafts for a user
# Get all drafts for a league
# Get a specified draft
# Get all picks in a draft
# Get traded picks in a draft


class SleeperDraftsAPI(RequestAPI):
    def __init__(self):
        # Allow using _call from RequestAPI
        super().__init__()

    def getAllDraftsForUser(self, season, user_id, sport="nfl"):
        """
        Retrieve all drafts by a user

        Args:
            season (int): Season or year
            user_id (str): Numerical ID of the user
            sport (str): (Default: "nfl") Sport, where only "nfl" is supported currently

        Returns:
            drafts: JSON data about all drafts for a specified user
        """
        try:
            url = f"https://api.sleeper.app/v1/user/{user_id}/drafts/{sport}/{season}"
            drafts = self._call(url)
            return drafts
        except APIException as e:
            logger.error(f"Error fetching drafts for user {user_id}: {e}")
            raise

    def getAllDraftsLeague(self, league_id):
        """
        Retrieve all drafts for a league

        Args:
            league_id (str): ID of Sleeper league for which you are trying to retrieve drafts

        Returns:
            drafts: JSON data about all drafts for a specified league
        """
        try:
            url = f"https://api.sleeper.app/v1/league/{league_id}/drafts"
            drafts = self._call(url)
            return drafts
        except APIException as e:
            logger.error(f"Error fetching drafts for user {user_id}: {e}")
            raise

    def getSpecificDraft(self, draft_id):
        """
        Retrieve a specific draft

        Args:
            draft_id (str): ID of draft

        Returns:
            drafts: JSON data about a specified draft
        """
        try:
            url = f"https://api.sleeper.app/v1/draft/{draft_id}"
            drafts = self._call(url)
            return drafts
        except APIException as e:
            logger.error(f"Error fetching drafts for user {user_id}: {e}")
            raise

    def getDraftPicks(self, draft_id):
        """
        Retrieve all picks in a draft

        Args:
            draft_id (str): ID of draft

        Returns:
            drafts: JSON data for all picks for a specified draft
        """
        try:
            url = f"https://api.sleeper.app/v1/draft/{draft_id}/picks"
            drafts = self._call(url)
            return drafts
        except APIException as e:
            logger.error(f"Error fetching drafts for user {user_id}: {e}")
            raise

    def getDraftTradedPicks(self, draft_id):
        """
        Retrieve all traded picks for a draft

        Args:
            draft_id (str): ID of draft

        Returns: JSON data with traded picks in a draft
        """
        try:
            url = f"https://api.sleeper.app/v1/draft/{draft_id}/traded_picks"
            drafts = self._call(url)
            return drafts
        except APIException as e:
            logger.error(f"Error fetching drafts for user {user_id}: {e}")
            raise
        getAllDraftsForUser(season, user_id)
