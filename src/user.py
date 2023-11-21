from request import RequestAPI, APIException
from utils.logger import Logger
import requests
import json
from datetime import datetime

# Configure logging
Logger.configureLog()

# Get a logger for this module
logger = Logger.getLog(__name__)


class SleeperUser(RequestAPI):
    def getUser(self, user):
        """
        Fetch user information from Sleeper API.

        Args:
            user (_type_): User ID or username

        Returns:
            user: Dictionary of user information
        """
        try:
            url = f"https://api.sleeper.app/v1/user/{user}"
            user_data = self._call(url)
            logger.info(f"Successfully fetched user data for {user}")
            return user_data
        except APIException as e:
            logger.error(f"Error fetching user data for {user}: {e}")
            raise
