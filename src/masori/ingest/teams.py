"""
Handles ingestion of NFL teams from ESPN api
"""

import requests
from typing import Dict

class Teams:
    def __init__(self):
        pass

    def get_espn_teams(self, team_id: str) -> None:
        """
        Retrieves NFL Team information from ESPN API in raw format
        """
        url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}'

        try:
            resp = requests.get(url)
            resp.raise_for_status()

            data = resp.json()

        except Exception as e:
            print(f'Problem getting team data for team_id {team_id} - {e}')
        
        return data

    def transform_espn_teams(self, team_data: Dict) -> Dict:
        """
        Transforms payload from https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}

        Schema is flexible - if addtl fields are needed, adjust in this function

        Args:
            team_data: str - raw string from api response
        
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
