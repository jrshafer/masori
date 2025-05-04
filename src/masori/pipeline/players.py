"""
Handles the ingestion pipelines for players related datasets
"""

import datetime
from typing import List, Dict
from loguru import logger

from masori.ingest.common import Common
from masori.ingest.players import Players
from masori.db.database import Database

class PlayerPipelines:
    def __init__(self):
        self.logger = logger
        self.year = datetime.datetime.now().year
        self.database_name = "nfl"
        self.database_schema = None
        self.table_name = None
        self.partition_keys = None

    def exec_player_reference_pipeline(self) -> List[Dict]:
        """
        Pipeline for teams.reference.teams pg table

        Args:
            schedule: str - cron string for pipeline to be ran
                TODO: add cron functionality for pipeline

        Returns:
            List[Dicts] - List of Dict of team data
                TODO: add database functionality as data sink
        """

         # create instance of necessary classes
        players_api = Players()
        common_utils = Common()
        database = Database()

        # set pipeline attributes
        self.database_schema = 'reference'
        self.table_name = 'players'
        self.partition_keys = ['id']
        
        fq_table_name = f'{self.database_name}.{self.database_schema}.{self.table_name}'
        self.logger.info(f'Starting data pipeline for {fq_table_name}')

        #  get list of ids to perform extraction on
        self.logger.info('Getting list of NFL team ids')
        team_ids = common_utils.get_nfl_team_metadata(self.year)

        # initialize list to append data to
        players_dataset = []


        for id in team_ids:
            self.logger.info(f'Getting roster for team id {id}')
            roster = players_api.get_espn_roster_by_team(id)

            athletes = roster.get('athletes')

            for group in athletes:
                for player in group.get('items', []):
                    transformed_player = players_api.transform_espn_roster(player)
                    players_dataset.append(transformed_player)

            self.logger.info(f'Finished transformation for team id {id}')

        self.logger.info(f'Finished transforming player data. Writing to database table {fq_table_name}')

        # write to database 
        database.upsert_table(
            database=self.database_name,
            schema=self.database_schema,
            table_name=self.table_name,
            rows=players_dataset,
            partition_keys=self.partition_keys
        )

        self.logger.info('Pipeline complete.')