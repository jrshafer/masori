"""
Handles the ingestion pipelines for position related datasets
"""

import datetime

from masori.ingest.common import Common
from masori.ingest.positions import Positions
from masori.pipeline.pipeline import GenericPipeline
from masori.db.database import Database

class PositionsPipelineRunner:
    def __init__(self):
        self.teams = Positions()
        self.common = Common()
        self.database = Database()

    def run(self):
        positions = GenericPipeline(
            pipeline_name='reference data [positions]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='reference',
            table_name='positions',
            partition_keys=['id'],
            id_fetcher=self.common.get_nfl_position_ids,
            extract_fn=self.teams.get_espn_positions,
            data_slicer=lambda raw: [raw],
            transform_fn=self.teams.transform_espn_positions
        )

        # parent_positions = GenericPipeline(
        #     pipeline_name='reference data [parent_positions]',
        #     year = datetime.datetime.now().year,
        #     database_name='nfl',
        #     schema='reference',
        #     table_name='parent_positions',
        #     partition_keys=['id'],
        #     id_fetcher=lambda ids: self.database.get_unique_ids(
        #         'reference',
        #         'positions',
        #         'id_parent_key'
        #     ),
        #     extract_fn=self.teams.get_espn_positions,
        #     data_slicer=lambda raw: [raw],
        #     transform_fn=self.teams.transform_espn_parent_positions
        # )

        pipelines = [
            positions, 
            # parent_positions
        ]

        for pipeline in pipelines:
            pipeline.run()
