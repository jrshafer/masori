"""
Handles ingestion of NFL teams from ESPN api
"""

from typing import Dict
from loguru import logger
from masori.ingest.common import Common

class Teams:
    def __init__(self):
        self.logger = logger
        self.common = Common()

    def get_espn_teams(self, team_id: str) -> Dict:
        """
        Retrieves NFL Team information from ESPN API in raw format
        """
        url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}'

        data = self.common.generic_http_request(url)
        
        return data

    def transform_espn_teams(self, team: Dict) -> Dict:
        """
        Transforms payload from https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}

        Schema is flexible - if addtl fields are needed, adjust in this function

        Args:
            team_data: str - raw string from api response

        payload structure:
            {
            "team": {
                    "id": "4",
                    "uid": "s:20~l:28~t:4",
                    "slug": "cincinnati-bengals",
                    "location": "Cincinnati",
                    "name": "Bengals",
                    "nickname": "Bengals",
                    "abbreviation": "CIN",
                    "displayName": "Cincinnati Bengals",
                    "shortDisplayName": "Bengals",
                    "color": "fb4f14",
                    "alternateColor": "000000",
                    "isActive": true,
                }
            }
        
        Returns:
            Dict{} - key value pair of data in normalized format
        """

        ret = {}

        ret = {
            'id': int(team['id']),
            's_name': str(team['displayName']),
            's_abbrev': str(team['abbreviation']),
            's_city': str(team['location']),
            's_team_name': str(team['name']),
            'b_is_active': bool(team['isActive'])
        }

        return ret
