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

    def transform_espn_teams(self, team_data: Dict) -> Dict:
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

        data = team_data.get('team', [])

        ret = {
            'id': int(data['id']),
            's_name': str(data['displayName']),
            's_abbrev': str(data['abbreviation']),
            's_city': str(data['location']),
            's_team_name': str(data['name']),
            'b_is_active': bool(data['isActive'])
        }

        return ret
