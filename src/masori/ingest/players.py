"""
Handles ingestion of NFL players from ESPN api
"""

from typing import Dict
from loguru import logger

from masori.ingest.common import Common

class Players:
    def __init__(self):
        self.logger = logger
        self.common = Common()

    def get_espn_roster_by_team(self, team_id: str) -> Dict:
        """
        Retrieves NFL Team information from ESPN API in raw format
        """
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster"

        data = self.common.generic_http_request(url)
        
        return data
        
    def transform_espn_roster(self, player: Dict) -> Dict:
        """
        Transforms payload from https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}

        Schema is flexible - if addtl fields are needed, adjust in this function

        Args:
            team_data: str - raw string from api response

        payload structure:
            "athletes": [
                {
                    "position": "offense",
                    "items": [
                        {
                        "id": "4427834",
                        "uid": "s:20~l:28~a:4427834",
                        "guid": "6d15a357-5fc2-f85d-8a9f-15f67ed3347a",
                        "alternateIds": {
                            "sdr": "4427834"
                        },
                        "firstName": "Erick",
                        "lastName": "All Jr.",
                        "fullName": "Erick All Jr.",
                        "displayName": "Erick All Jr.",
                        "shortName": "E. All Jr.",
                        "weight": 253,
                        "displayWeight": "253 lbs",
                        "height": 77,
                        "displayHeight": "6' 5\"",
                        "age": 24,
                        "dateOfBirth": "2000-09-13T07:00Z"
                }
        
        Returns:
            Dict{} - key value pair of data in normalized format
        """

        ret = {}

        team_ref = player.get('teams', [{}])[0].get('$ref')
        team_id = self.common.parse_ref_string_for_id(team_ref) if team_ref else None

        ret = {
            'id': int(player['id']),
            's_first_name': str(player['lastName']),
            's_last_name': str(player['lastName']),
            's_full_name': str(player['fullName']),
            'id_team_key': int(team_id) if team_id else None,
            's_position_name': str(player['position']['name']),
            's_position_abbrev': str(player['position']['abbreviation']),
            'id_position_key': int(player['position']['id'])
        }

        return ret
