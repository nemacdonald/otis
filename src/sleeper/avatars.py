from sleeper.request import RequestAPI, APIException
from utils.logger import Logger
import requests
import json
from datetime import datetime

# Configure logging
Logger.configureLog()

# Get a logger for this module
logger = Logger.getLog(__name__)


class SleeperAvatarsAPI(RequestAPI):
    def getAvatarFullSize(self, avatar_id):
        try:
            url = f"https://sleepercdn.com/avatars/{avatar_id}"
            avatar = self._call(url)
            logger.info(f"Successfully fetched avatar for {avatar_id}")
            return user_data
        except APIException as e:
            logger.error(f"Error fetching user data for {avatar_id}: {e}")
            raise

    def getAvatarThumbnail(self, avatar_id):
        try:
            url = f"https://sleepercdn.com/avatars/{avatar_id}"
            avatar = self._call(url)
            logger.info(f"Successfully fetched avatar for {avatar_id}")
            return user_data
        except APIException as e:
            logger.error(f"Error fetching user data for {avatar_id}: {e}")
            raise
