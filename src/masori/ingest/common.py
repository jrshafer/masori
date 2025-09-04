"""
Handles common functions for data ingestion
"""

import requests
from typing import List, Dict, Any
from loguru import logger
from bs4 import BeautifulSoup
from datetime import datetime
import re
import io
import csv

class Common:
    def __init__(self):
        self.logger = logger

    @staticmethod
    def determine_nfl_week():
        """
        Determine week for fantasy data retrieval.
        Args:
            None
        Returns:
            week (int): Integer representing the football week
        """
        today = datetime.now().date()

        year = today.year
        season_start = datetime(year, 9, 4).date() 
        delta = (today - season_start).days
        week = delta // 7 + 1

        if week < 1:
            return 1
        if week > 18:
            return 18
        return week
    
    @staticmethod
    def determine_year():
        """
        Return year - handy for tagging years in data pipes
        Args:
            None
        Return:
            year (int): Integer representing the current year
        """

        return datetime.now().date().year


    @staticmethod
    def split_player_and_team(text: str):
        """
        Split a FantasyPros Player cell into (player_name, team_abbrev).
        Example: 'Jalen HurtsPHI' -> ('Jalen Hurts', 'PHI')
                'Travis KelceKC' -> ('Travis Kelce', 'KC')
        """
        m = re.match(r"^(.*?)([A-Z]{2,3})$", text.strip())
        if m:
            return m.group(1).strip(), m.group(2)
        return text.strip(), None

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
    
    def generic_csv_request(self, url: str) -> Dict:
        """
        Generic csv request for use in many csv sources

        Args:
            url: str - url to make the request to
        
        Returns:
            Dict - dictionary of data from json response
        """
        try:
            resp = requests.get(url)
            resp.raise_for_status()

            text = resp.text

            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)

        except Exception as e:
            logger.warning(f'Problem making http request to url {url} - {e}')

        return rows
    
    def generic_fantasypros_html_parser(self, url: str) -> Dict:
        """
        Generic HTML parser for FantasyPros tables that separates Player and Team.
        """
        data: Dict[str, Any] = {}
        try:
            resp = requests.get(url)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.content, features="lxml")
            elements = soup.find_all("div", {"class": "mobile-table"})

            parsed_results: List[Dict[str, Any]] = []

            for el in elements:
                table = el.find("table")
                if not table:
                    continue

                headers = [th.get_text(strip=True) for th in table.find_all("th")]

                for row in table.find_all("tr"):
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if not cells or not headers:
                        continue

                    row_dict = dict(zip(headers, cells))

                    # If Player field exists, split into Player and Team
                    if "Player" in row_dict and row_dict["Player"]:
                        player_name, team = self.split_player_and_team(row_dict["Player"])
                        row_dict["Player"] = player_name
                        if team:
                            row_dict["Team"] = team

                    parsed_results.append(row_dict)

            data["results"] = parsed_results

        except Exception as e:
            logger.warning(f"Problem making HTML request to {url} - {e}")

        return data

    
    def get_nfl_season_years(self, year) -> List[str]:
        """
        Scrapes espn api for NFL season years and returns them as a list

        By default only returns last 5 years - mainly for testing purposes

        i.e season types

        Args:
            url: str - url with year needed
        
        Returns:
            List[Any] - list of objects for generic pipeline class to iterate over
        """
        print(f'I should fix this : year {year}')

        url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons?limit=100"

        resp = self.generic_http_request(url)
        ret = []
        
        
        MAX_COUNT = 5 #adjust to add more seasons
        COUNT = 0

        for season in resp.get("items", []):
            szn = self.parse_ref_string_for_id(season["$ref"])
            ret.append(szn)
            COUNT += 1

            if COUNT == MAX_COUNT:
                return ret

    
    def parse_ref_string_for_id(self, ref_string: str) -> int:
        """
        Parses a $ref string and returns the ID
        """
        try:
           return ref_string.split('/')[-1].split('?')[0]
        except (ValueError, IndexError):
            return None
         

    # def generic_espn_api_metadata_request(self, url: str, item_key: str="items",
    #                                       dict_key: str="$ref") -> List[str]:
    #     try:
    #         resp = requests.get(url)
    #         resp.raise_for_status()
    #         data = resp.json()

    #         ret = []

    #         for entry in data.get(item_key, []):
    #             id = self.parse_ref_string_for_id(entry[dict_key])
    #             ret.append(id)

    #     except Exception as e:
    #         logger.warning(f'Problem making http request to url {url} - {e}')

    #     return ret

    def generic_espn_api_metadata_request(
        self,
        url: str,
        item_key: str = "items",
        dict_key: str = "$ref"
    ) -> List[str]:
        """
        Fetches all items from a paginated ESPN API endpoint and returns a list of IDs.

        Args:
            url: Base ESPN API endpoint
            item_key: Key in the JSON where items are stored (default: "items")
            dict_key: Key in each item dict containing the reference URL (default: "$ref")

        Returns:
            List[str]: List of parsed IDs from all pages
        """
        all_ids = []
        page = 1

        while True:
            paged_url = f"{url}?page={page}"
            try:
                resp = requests.get(paged_url)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                self.logger.warning(f"Problem fetching page {page} from {url}: {e}")
                break

            items = data.get(item_key, [])
            for entry in items:
                try:
                    id = self.parse_ref_string_for_id(entry[dict_key])
                    all_ids.append(id)
                except KeyError:
                    self.logger.warning(f"Missing expected key '{dict_key}' in entry: {entry}")

            if page >= data.get("pageCount", 1):
                break
            page += 1

        return all_ids