"""
Handles ingestion of fantasy projection data from fantasypros.com
"""

from typing import Dict
from loguru import logger

from masori.ingest.common import Common

class Fantasy:
    def __init__(self):
        self.logger = logger
        self.common = Common()

    def get_data_from_fantasypros(self, position: str) -> Dict:
        """
        Scrapes https://www.fantasypros.com for fantasy data.
        Args:
            position (str): Position to scrape data for.
        Returns:
            df (DataFrame): Pandas DataFrame with player and fantasy points projections.

        """
        week = self.common.determine_nfl_week()
        url = f'https://www.fantasypros.com/nfl/projections/{position}.php?week={week}&scoring=PPR'
        

        resp = self.common.generic_fantasypros_html_parser(url)

        return resp
    
    def transform_qb_data_from_fantasypros(self, results: Dict) -> Dict:
        '''
        transforms payload from https://www.fantasypros.com/nfl/projections/qb.php?week={week}&scoring=PPR

                Schema is flexible - if addtl fields are needed, adjust in this function

                Args:
                    player: str - raw string from api response

                payload structure:
        {
            "results": [
                {
                    "Player": "Brock Purdy",
                    "ATT": "3.5",
                    "CMP": "21.4",
                    "YDS": "14.8",
                    "TDS": "0.2",
                    "INTS": "0.8",
                    "FL": "0.2",
                    "FPTS": "17.7",
                    "Team": "SF"
                },
            ]
        }   


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': results['Player'],
                'dec_att': float(results['ATT']),
                'dec_cmp': float(results['CMP']),
                'dec_yds': float(results['YDS']),
                'dec_tds': float(results['TDS']),
                'dec_ints': float(results['INTS']),
                'dec_fl': float(results['FL']),
                'dec_fpts': float(results['FPTS']),
                's_team': results['Team'],
                'id_week': int(week),
                'id_year': int(year)

            }
        except Exception as e:
            logger.warning(f'incomplete data record: {results} - {e}')
            return {}

        
        return ret


    def transform_wr_data_from_fantasypros(self, results: Dict) -> Dict:
        '''
        transforms payload from https://www.fantasypros.com/nfl/projections/wr.php?week={week}&scoring=PPR

                Schema is flexible - if addtl fields are needed, adjust in this function

                Args:
                    player: str - raw string from api response

                payload structure:
        {
            "results": [
                {
                    "Player": "Ja'Marr Chase",
                    "REC": "6.7",
                    "YDS": "1.2",
                    "TDS": "0.0",
                    "ATT": "0.2",
                    "FL": "0.1",
                    "FPTS": "19.5",
                    "Team": "CIN"
                }
            ]
        }   


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': results['Player'],
                'dec_rec': float(results['REC']),
                'dec_yds': float(results['YDS']),
                'dec_tds': float(results['TDS']),
                'dec_att': float(results['ATT']),
                'dec_fl': float(results['FL']),
                'dec_fpts': float(results['FPTS']),
                's_team': results['Team'],
                'id_week': int(week),
                'id_year': int(year)

            }
        except Exception as e:
            logger.warning(f'incomplete data record: {results} - {e}')
            return {}

        
        return ret
    

    def transform_te_data_from_fantasypros(self, results: Dict) -> Dict:
        '''
        transforms payload from https://www.fantasypros.com/nfl/projections/te.php?week={week}&scoring=PPR

                Schema is flexible - if addtl fields are needed, adjust in this function

                Args:
                    player: str - raw string from api response

                payload structure:
        {
            "results": [
                {
                    "Player": "Trey McBride",
                    "REC": "6.2",
                    "YDS": "62.0",
                    "TDS": "0.4",
                    "FL": "0.0",
                    "FPTS": "14.6",
                    "Team": "ARI"
                }
            ]
        }   


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': results['Player'],
                'dec_rec': float(results['REC']),
                'dec_yds': float(results['YDS']),
                'dec_tds': float(results['TDS']),
                'dec_fl': float(results['FL']),
                'dec_fpts': float(results['FPTS']),
                's_team': results['Team'],
                'id_week': int(week),
                'id_year': int(year)

            }
        except Exception as e:
            logger.warning(f'incomplete data record: {results} - {e}')
            return {}

        
        return ret

    def transform_rb_data_from_fantasypros(self, results: Dict) -> Dict:
        '''
        transforms payload from https://www.fantasypros.com/nfl/projections/te.php?week={week}&scoring=PPR

                Schema is flexible - if addtl fields are needed, adjust in this function

                Args:
                    player: str - raw string from api response

                payload structure:
        {
            "results": [
                {
                    "Player": "Saquon Barkley",
                    "ATT": "20.3",
                    "YDS": "18.9",
                    "TDS": "0.1",
                    "REC": "2.4",
                    "FL": "0.1",
                    "FPTS": "19.0",
                    "Team": "PHI"
                }
            ]
        }   


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': results['Player'],
                'dec_att': float(results['ATT']),
                'dec_yds': float(results['YDS']),
                'dec_tds': float(results['TDS']),
                'dec_rec': float(results['REC']),
                'dec_fl': float(results['FL']),
                'dec_fpts': float(results['FPTS']),
                's_team': results['Team'],
                'id_week': int(week),
                'id_year': int(year)
            }

        except Exception as e:
            logger.warning(f'incomplete data record: {results} - {e}')
            return {}

        
        return ret
    

    def transform_dst_data_from_fantasypros(self, results: Dict) -> Dict:
        '''
        transforms payload from https://www.fantasypros.com/nfl/projections/te.php?week={week}&scoring=PPR

                Schema is flexible - if addtl fields are needed, adjust in this function

                Args:
                    player: str - raw string from api response

                payload structure:
        {
            "results": [
                {
                    "Player": "Pittsburgh Steelers",
                    "SACK": "2.9",
                    "INT": "0.8",
                    "FR": "0.6",
                    "FF": "1.0",
                    "TD": "0.2",
                    "SAFETY": "0.0",
                    "PA": "17.3",
                    "YDS AGN": "300.6",
                    "FPTS": "7.7"
                }
            ]
        }   


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': results['Player'],
                'dec_sack': float(results['SACK']),
                'dec_int': float(results['INT']),
                'dec_fr': float(results['FR']),
                'dec_ff': float(results['FF']),
                'dec_td': float(results['TD']),
                'dec_safety': float(results['SAFETY']),
                'dec_pa': float(results['PA']),
                'dec_yds_agn': results['YDS AGN'],
                'dec_fpts': float(results['FPTS']),
                'id_week': int(week),
                'id_year': int(year)
            }
            
        except Exception as e:
            logger.warning(f'incomplete data record: {results} - {e}')
            return {}

        
        return ret
    

    def transform_k_data_from_fantasypros(self, results: Dict) -> Dict:
        '''
        transforms payload from https://www.fantasypros.com/nfl/projections/k.php?week={week}&scoring=PPR

                Schema is flexible - if addtl fields are needed, adjust in this function

                Args:
                    player: str - raw string from api response

                payload structure:
        {
    "results": [
                {
                    "Player": "Jake Elliott",
                    "FG": "1.7",
                    "FGA": "1.9",
                    "XPT": "2.9",
                    "FPTS": "8.1",
                    "Team": "PHI"
                }
            ]
        }   


        Returns:
            Dict{} - key value pair of data in normalized format
            '''
        
        ret = {}
        _week = self.common.determine_nfl_week()
        week = 0 if _week == 'draft' else int(_week)
        year = self.common.determine_year()
        
        try:
            ret = {
                's_full_name': results['Player'],
                'dec_fg': float(results['FG']),
                'dec_fga': float(results['FGA']),
                'dec_xpt': float(results['XPT']),
                'dec_fpts': float(results['FPTS']),
                's_team': results['Team'],
                'id_week': int(week),
                'id_year': int(year)
            }
            
        except Exception as e:
            logger.warning(f'incomplete data record: {results} - {e}')
            return {}

        
        return ret