"""
Handles ingestion of NFL schedule from ESPN api
"""

from typing import Dict
from loguru import logger
from masori.ingest.common import Common

class Seasons:
    def __init__(self):
        self.logger = logger
        self.common = Common()

    def get_espn_season_types(self, year: str) -> Dict:
        """
        Retrieves NFL position information from ESPN API in raw format
        """
        url = f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}?lang=en&region=us"

        data = self.common.generic_http_request(url)
        
        return data
    
    def transform_espn_season_types(self, season) -> Dict:
        """
         Transforms payload from https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}

        Schema is flexible - if addtl fields are needed, adjust in this function

        Args:
            team_data: str - raw string from api response

        payload structure:
            {
                "$ref": "http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/1?lang=en&region=us",
                "id": "1",
                "type": 1,
                "name": "Preseason",
                "abbreviation": "pre",
                "year": 2024,
                "startDate": "2024-08-01T07:00Z",
                "endDate": "2024-09-05T06:59Z",
            }
        Returns:
            Dict{} - key value pair of data in normalized format
        
        """

        ret = {}

        ret = {
            'id': int(season['id']),
            's_name': str(season['name']),
            's_abbreviation': str(season['abbreviation'])
        }

        return ret
