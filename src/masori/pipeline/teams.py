"""
Handles the ingestion pipelines for teams related datasets
"""

import datetime
from typing import List, Dict
from loguru import logger

from masori.ingest.common import Common
from masori.ingest.teams import Teams
from masori.db.database import Database

class TeamPipelines:
    def __init__(self):
        self.logger = logger
        self.year = datetime.datetime.now().year
        self.database_name = "nfl"
        self.database_schema = None
        self.table_name = None
        self.partition_keys = None

    def exec_team_reference_pipeline(self) -> List[Dict]:
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
        teams_api = Teams()
        common_utils = Common()
        database = Database()

        # set pipeline attributes
        self.database_schema = 'reference'
        self.table_name = 'teams'
        self.partition_keys = ['id']
        
        table_name = f'{self.database_name}.reference.teams'
        self.logger.info(f'Starting data pipeline for {table_name}')

        #  get list of ids to perform extraction on
        self.logger.info('Getting list of NFL team ids')
        team_ids = common_utils.get_nfl_team_metadata(self.year)

        # initialize list to append data to
        teams_dataset = []


        for id in team_ids:
    
            logger.info(f'Getting data for ID {id}')
            raw_data = teams_api.get_espn_teams(id)

            logger.info(f'Got data for ID {id} - transforming to table structure')
            transformed_data = teams_api.transform_espn_teams(raw_data)

            logger.info(f'Transformation complete for ID {id}')
            teams_dataset.append(transformed_data)

        # write to database 
        database.upsert_table(
            database=self.database_name,
            schema=self.database_schema,
            table_name=self.table_name,
            rows=teams_dataset,
            partition_keys=self.partition_keys
        )

        self.logger.info('Pipeline complete.')