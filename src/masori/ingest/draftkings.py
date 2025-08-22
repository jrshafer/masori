"""
Handles ingestion of fantasy projection data from fantasypros.com
"""

from typing import Dict
from loguru import logger

from masori.ingest.common import Common

class Draftkings:
    def __init__(self):
        self.logger = logger
        self.common = Common()

    def get_draftkings_group_id(self, year) -> int:
        """
        Scrapes https://www.draftkings.com/lobby/getcontests?sport=nfl for group ID to retrieve DK salary data.
        Args:
            None
        Returns:
            group_id (int) group ID for DK contests

        """
        self.logger.debug(f'Heres the year: {year}')
        data = self.common.generic_http_request('https://www.draftkings.com/lobby/getcontests?sport=nfl')

        contests = data.get('Contests', [])

        for contest in contests:
            if "preseason" not in contest.get('n', "").lower() and contest.get('gameType', "") == "Classic" and contest.get('s') == 1 and contest.get('dg') != "":
                group_id = contest.get('dg')

                break

        return [group_id]
    
    def get_data_from_draftkings(self, group_id: str) -> Dict:
        """
        Scrapes https://www.draftkings.com for draftkings salary data.
        Args:
            group_id (str): DK group ID to get salaries for.
        Returns:
            data (dict): Salary data for the mfers

        """
        url = f"https://www.draftkings.com/lineup/getavailableplayerscsv?contestTypeId=21&draftGroupId={group_id}"

        resp = self.common.generic_csv_request(url)

        return resp
    
    def transform_data_from_draftkings(self, data: Dict) -> Dict:
        '''
        transforms payload from https://www.draftkings.com/lineup/getavailableplayerscsv?contestTypeId=21&draftGroupId={group_id}

        Schema is flexible - if addtl fields are needed, adjust in this function

        Args:
            player: str - raw string from api response

                payload structure:
        {
            "Position": "DST",
            "Name + ID": "Panthers  (39507011)",
            "Name": "Panthers ",
            "ID": "39507011",
            "Roster Position": "DST",
            "Salary": "2400",
            "Game Info": "CAR@JAX 09/07/2025 01:00PM ET",
            "TeamAbbrev": "CAR",
            "AvgPointsPerGame": "2.41"
        },


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': str(data['Name']).strip(),
                's_position': str(data['Position']).strip(),
                'i_salary': int(data['Salary']),
                'id_week': int(week),
                'id_year': int(year)
            }
        except Exception as e:
            logger.warning(f'incomplete data record: {data} - {e}')
            return {}
    
        return ret
