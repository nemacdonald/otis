import json
import datetime
import os
from sleeper.request import RequestAPI, APIException
from utils.logger import LoggingConfig

# Configure logging
LoggingConfig.configureLog()

# Get a logger for this module
logger = LoggingConfig.getLog(__name__)


class SleeperPlayersAPI(RequestAPI):
    def __init__(self, path="projects/otis/src/players"):
        super().__init__()
        self.path = path
        home = os.path.expanduser("~")
        self.players_file = f"sleeper_players_{self.getCurrentDate()}.json"
        self.data_filename = os.path.join(home, self.path, self.players_file)

    def getCurrentDate(self):
        return datetime.datetime.now().strftime("%Y-%m-%d")

    def fetchAllPlayers(self):
        """
        Fetch all players.

        Args:


        Returns:
            dict: Information about the specified league.
        """
        try:
            # Check if data has already been fetched today
            if self.checkLastFetch():
                logger.info(
                    f"Players have already been fetched today ({self.getCurrentDate()}). Reading {self.data_filename}"
                )
                with open(self.data_filename, "r") as json_file:
                    players = json.load(json_file)
            else:
                url = f"https://api.sleeper.app/v1/players/nfl"
                players = self._call(url)
                logger.info(f"Successfully fetched all players")

                # Save players to JSON file
                self.saveToJSON(players, self.data_filename)
                logger.info(
                    f"Successfully saved all players data: {self.data_filename}"
                )

                logger.warning(
                    "Please use this call sparingly, as it is intended only to be used once per day at most to keep your player IDs updated. The average size of this query is 5MB."
                )

            return players
        except APIException as e:
            logger.error(f"Error fetching players: {e}")
            raise

    def checkLastFetch(self):
        try:
            with open(self.data_filename, "r") as json_file:
                data = json.load(json_file)
                last_fetch_date = data.get("last_fetch_date", "")
                return last_fetch_date == self.getCurrentDate()
        except (FileNotFoundError, json.JSONDecodeError):
            return False

    def saveToJSON(self, data, filename):
        """
        Save data to a JSON file.

        Args:
            data (dict): Data to be saved.
            filename (str): Name of the JSON file.
        """
        data["last_fetch_date"] = self.getCurrentDate()
        with open(filename, "w") as json_file:
            json.dump(data, json_file)
            logger.info(f"Data saved to {filename}")
