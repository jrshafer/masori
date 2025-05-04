"""
Handles common functions for data ingestion
"""

import requests
from typing import List

class Common:
    def __init__(self):
        pass

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
