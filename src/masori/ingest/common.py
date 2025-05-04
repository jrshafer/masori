"""
Handles common functions for data ingestion
"""

import requests
from typing import List, Dict
from loguru import logger

class Common:
    def __init__(self):
        self.logger = logger

    def get_nfl_team_metadata(self, year: str) -> List[str]:
        """
        Scrapes espn api for teams in an NFL season and returns their ESPN API IDs
        
        Args:
            year: str - nfl position to retrieve

        Returns:
            list[str]: list of team IDs for the given season
        """

        url = f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/teams'
        ret = []

        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()

            for entry in data.get("items", []):
                id = entry["$ref"].split('/')[-1].split('?')[0]
                ret.append(id)

        except Exception as e:
            print(f'Encountered a problem - {e}')
        
        return ret


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
    
    def parse_team_string_for_id(self, team_string: str) -> str:
        return team_string.split('/')[-1].split('?')[0]
