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
