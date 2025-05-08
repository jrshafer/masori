"""
Handles common functions for data ingestion
"""

import requests
from typing import List, Dict
from loguru import logger

class Common:
    def __init__(self):
        self.logger = logger

    def get_nfl_team_ids(self, year: str) -> List[str]:
        """
        Scrapes espn api for teams in an NFL season and returns their ESPN API IDs
        
        Args:
            year: str - nfl position to retrieve

        Returns:
            list[str]: list of team IDs for the given season
        """

        url = f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/teams'
        
        return self.generic_espn_api_metadata_request(
            url=url
        )
    
    def get_nfl_position_ids(self, year: str) -> List[str]:
        """
        Scrapes espn api for NFL positions and returns their ESPN API IDs

        Returns:
            list[str]: list of position IDs
        """

        url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions?limit=100&ignore={year}"

        return self.generic_espn_api_metadata_request(
            url=url
        )

    def generic_http_request(self, url: str) -> Dict:
        """
        Generic http request for use in many api sources

        Args:
            url: str - url to make the request to
        
        Returns:
            Dict - dictionary of data from json response
        """
        try:
            resp = requests.get(url)
            resp.raise_for_status()

            data = resp.json()

        except Exception as e:
            logger.warning(f'Problem making http request to url {url} - {e}')

        return data
    
    def parse_ref_string_for_id(self, ref_string: str) -> int:
        """
        
        """
        try:
           return ref_string.split('/')[-1].split('?')[0]
        except (ValueError, IndexError):
            return None
         

    def generic_espn_api_metadata_request(self, url: str, item_key: str="items",
                                          dict_key: str="$ref") -> List[str]:
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()

            ret = []

            for entry in data.get(item_key, []):
                id = self.parse_ref_string_for_id(entry[dict_key])
                ret.append(id)

        except Exception as e:
            logger.warning(f'Problem making http request to url {url} - {e}')

        return ret


common = Common()

print(common.parse_ref_string_for_id('http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/teams/4?lang=en&region=us'))