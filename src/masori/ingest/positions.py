"""
Handles ingestion of NFL positions from ESPN api
"""

from typing import Dict
from loguru import logger

from masori.ingest.common import Common

class Positions:
    def __init__(self):
        self.logger = logger
        self.common = Common()

    def get_espn_positions(self, position_id: str) -> Dict:
        """
        Retrieves NFL position information from ESPN API in raw format
        """
        url = f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/{position_id}?lang=en&region=us"

        data = self.common.generic_http_request(url)
        
        return data
        
    def transform_espn_positions(self, position: Dict) -> Dict:
        """
        Transforms payload from https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}

        Schema is flexible - if addtl fields are needed, adjust in this function

        Args:
            team_data: str - raw string from api response

        payload structure:
            {
                "$ref": "http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/1?lang=en&region=us",
                "id": "1",
                "name": "Wide Receiver",
                "displayName": "Wide Receiver",
                "abbreviation": "WR",
                "leaf": true,
                "parent": {
                    "$ref": "http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/70?lang=en&region=us"
                }
            }
        Returns:
            Dict{} - key value pair of data in normalized format
        """

        ret = {}

        parent_pos_ref = position.get('parent', {}).get('$ref', 'NA')


        parent_id = self.common.parse_ref_string_for_id(parent_pos_ref) if parent_pos_ref != 'NA' else None

        ret = {
            'id': int(position['id']),
            's_position_name': str(position['name']),
            's_abbreviation': str(position['abbreviation']),
            'id_parent_key': int(parent_id) if parent_id is not None else None
        }

        return ret